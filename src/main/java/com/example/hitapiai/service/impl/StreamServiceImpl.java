package com.example.hitapiai.service.impl;

import com.example.hitapiai.configuration.webClient.upStreamClient;
import com.example.hitapiai.exception.StreamException;
import com.example.hitapiai.model.Conversation;
import com.example.hitapiai.model.Message;
import com.example.hitapiai.model.User;
import com.example.hitapiai.payload.SSE.ChatEvent;
import com.example.hitapiai.payload.request.StreamEventRequest;
import com.example.hitapiai.repository.ConversationRepository;
import com.example.hitapiai.repository.UserRepository;
import com.example.hitapiai.service.ReactiveChatPersistence;
import com.example.hitapiai.service.StreamService;
import com.example.hitapiai.utils.FilterText;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Service
@RequiredArgsConstructor
public class StreamServiceImpl implements StreamService {

    private static final int MAX_BUFFER_CHARS = 200_000;

    private final upStreamClient upStreamClient;
    private final ObjectMapper om;
    private final FilterText filterText;

    private final UserRepository userRepository;
    private final ConversationRepository conversationRepository;

    private final ReactiveChatPersistence persistence;

    @Override
    public Flux<ServerSentEvent<ChatEvent>> streamLive(StreamEventRequest req) {
        if (req == null || req.getInput() == null) {
            return Flux.just(sse("error", ChatEvent.error(null, null, null, "Invalid request payload")));
        }

        String userId = req.getInput().getUser_id();
        if (userId == null || userId.isBlank()) {
            return Flux.just(sse("error", ChatEvent.error(null, null, null, "User ID is required")));
        }
        String tempCid = req.getInput().getConversation_id();
        
        // Pastikan ID digenerate di awal jika kosong (menggunakan format UUID)
        if (tempCid == null || tempCid.isBlank()) {
            tempCid = java.util.UUID.randomUUID().toString();
            req.getInput().setSessionId(tempCid); // Update req untuk dikirim ke upstream
            log.info("Generated new session_id (UUID): {}", tempCid);
        }
        
        final String initialConversationId = tempCid;
        String question = filterText.extractUserText(req);

        // ===== state per request =====
        StreamState state = new StreamState(initialConversationId);

        // 1) load user (Reactive)
        Mono<User> userMono = userRepository.findById(userId)
                .switchIfEmpty(Mono.error(new StreamException("User not found: " + userId)))
                .cache();

        // 2) Load or Create conversation immediately
        Mono<Conversation> convoMono = userMono.flatMap(user ->
                persistence.ensureConversation(user.getId(), initialConversationId, titleFromQuestion(question))
        ).cache();

        // 3) Save User Message & Create Assistant Placeholder (In Parallel with Upstream)
        Mono<Void> contextInitMono = convoMono.flatMap(convo -> {
            log.info("Conversation ensured: {}", convo.getConversationId());
            return persistence.saveUserMessage(convo.getConversationId(), question)
                    .flatMap(userMsg -> {
                        log.info("User message saved: {}", userMsg.getId());
                        return persistence.createAssistantPlaceholder(convo.getConversationId());
                    })
                    .map(placeholder -> {
                        state.assistantMsgPk.set(placeholder.getId());
                        state.ctxInit.set(true);
                        log.info("Assistant placeholder created: {}", placeholder.getId());
                        return placeholder;
                    });
        }).then();

        // 4) streaming upstream -> SSE
        Flux<ServerSentEvent<ChatEvent>> upstreamSse =
                contextInitMono.thenMany(Flux.defer(() -> upStreamClient.streamNdjsonLines(req)))
                .concatMap(line -> {
                    if (line == null) {
                        return Flux.empty();
                    }
                    log.info("Upstream line raw: {}", line);
                    String normalized = line.trim();
                    if (normalized.isEmpty() || normalized.startsWith("event:")) {
                        return Flux.empty();
                    }
                    if (normalized.startsWith("data:")) {
                        normalized = normalized.substring(5).trim();
                    }

                    if (normalized.equals("[DONE]")) {
                        log.info("Received [DONE] from upstream");
                        return Flux.empty();
                    }

                    final JsonNode root;
                    try {
                        root = om.readTree(normalized);
                    } catch (Exception e) {
                        log.error("Failed to parse JSON from upstream: '{}'. Error: {}", normalized, e.getMessage());
                        return Flux.just(sse("error", ChatEvent.error(
                                state.conversationId.get(), state.assistantMsgPk.get(), state.runId.get(),
                                "Invalid upstream JSON: " + e.getMessage()
                        )));
                    }

                    String event = root.path("event").asText("");
                    String runId = root.path("run_id").asText(null);
                    if (runId == null || runId.isBlank()) {
                        runId = root.path("data").path("run_id").asText(null);
                    }
                    if (runId != null && !runId.isBlank()) state.runId.compareAndSet(null, runId);

                    // Log event metadata to DB in background
                    persistence.logStreamEvent(state.conversationId.get(), state.runId.get(), event, normalized)
                            .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                            .subscribe(null, err -> log.error("Failed to log stream event: {}", err.getMessage()));

                    if ("metadata".equals(event)) {
                        if (state.metaEmitted.compareAndSet(false, true)) {
                            return Flux.just(sse("metadata", ChatEvent.metadata(
                                    state.conversationId.get(), state.assistantMsgPk.get(), state.runId.get()
                            )));
                        }
                        return Flux.empty();
                    }

                    // upstream kamu utama: on_chat_model_stream
                    if (!"on_chat_model_stream".equals(event)) {
                        return Flux.empty();
                    }

                    String delta = root.path("data").path("chunk").path("content").asText("");
                    if (delta == null || delta.isBlank()) return Flux.empty();

                    Flux<ServerSentEvent<ChatEvent>> meta = Flux.empty();
                    if (state.metaEmitted.compareAndSet(false, true)) {
                        meta = Flux.just(sse("metadata", ChatEvent.metadata(
                                state.conversationId.get(), state.assistantMsgPk.get(), state.runId.get()
                        )));
                    }

                    // buffer append with limit
                    if (!state.truncated.get()) {
                        StringBuilder sb = state.buffer.get();
                        int remaining = MAX_BUFFER_CHARS - sb.length();
                        if (remaining > 0) {
                            if (delta.length() <= remaining) {
                                sb.append(delta);
                            } else {
                                sb.append(delta, 0, remaining);
                                state.truncated.set(true);
                            }
                            log.info("Delta appended: '{}', current buffer length: {}", delta, sb.length());
                        } else {
                            state.truncated.set(true);
                        }
                    }

                    Flux<ServerSentEvent<ChatEvent>> chunk = Flux.just(
                            sse("chunk", ChatEvent.chunk(
                                    state.conversationId.get(), state.assistantMsgPk.get(), state.runId.get(), delta
                            ))
                    );

                    return Flux.concat(meta, chunk);
                })
                .onErrorResume(err -> {
                    log.error("Streaming error: ", err);
                    return Flux.just(
                            sse("error", ChatEvent.error(
                                    state.conversationId.get(), state.assistantMsgPk.get(), state.runId.get(),
                                    err.getMessage()
                            ))
                    );
                });

        // 5) finalize assistant at end
        Flux<ServerSentEvent<ChatEvent>> finalize =
                Mono.defer(() -> {
                    Long asstId = state.assistantMsgPk.get();
                    String finalText = state.buffer.get().toString();
                    log.info("Finalizing assistant message. id={}, length={}", asstId, finalText.length());
                    if (asstId != null) {
                        if (state.truncated.get()) finalText = finalText + "\n\n[TRUNCATED]";
                        return persistence.finalizeAssistantMessage(asstId, finalText)
                                .thenReturn(sse("done", ChatEvent.done(
                                        state.conversationId.get(), asstId, state.runId.get()
                                )));
                    }
                    return Mono.just(sse("done", ChatEvent.done(
                            state.conversationId.get(), asstId, state.runId.get()
                    )));
                })
                .flux();

        return upstreamSse
                .concatWith(finalize)
                .doFinally(sig -> {
                    if (sig == reactor.core.publisher.SignalType.CANCEL) {
                        log.warn("Client disconnected. conversationId={}, runId={}", state.conversationId.get(), state.runId.get());
                    }
                });
    }

    private String titleFromQuestion(String q) {
        if (q == null) return "New Chat";
        String s = q.strip();
        return s.length() <= 60 ? s : s.substring(0, 60);
    }

    private static class StreamState {
        final AtomicReference<String> runId = new AtomicReference<>(null);
        final AtomicReference<String> conversationId;
        final AtomicReference<Long> assistantMsgPk = new AtomicReference<>(null);
        final AtomicReference<StringBuilder> buffer = new AtomicReference<>(new StringBuilder());
        final AtomicBoolean truncated = new AtomicBoolean(false);
        final AtomicBoolean ctxInit = new AtomicBoolean(false);
        final AtomicBoolean metaEmitted = new AtomicBoolean(false);
        final AtomicBoolean convoExistedAtStart = new AtomicBoolean(false);

        StreamState(String initialConversationId) {
            this.conversationId = new AtomicReference<>(initialConversationId);
        }
    }

    private ServerSentEvent<ChatEvent> sse(String event, ChatEvent data) {
        return ServerSentEvent.<ChatEvent>builder().event(event).data(data).build();
    }
}
