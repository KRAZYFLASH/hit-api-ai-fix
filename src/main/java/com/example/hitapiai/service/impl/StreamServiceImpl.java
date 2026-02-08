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
import com.example.hitapiai.service.BlockingChatPersistence;
import com.example.hitapiai.service.StreamService;
import com.example.hitapiai.utils.FilterText;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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

    private final BlockingChatPersistence persistence;

    @Override
    public Flux<ServerSentEvent<ChatEvent>> streamLive(StreamEventRequest req) {

        String userId = req.getInput().getUser_id();
        String initialSessionId = req.getInput().getSession_id();
        String question = filterText.extractUserText(req);

        // ===== state per request =====
        AtomicReference<String> runIdRef = new AtomicReference<>(null);
        AtomicReference<String> sessionIdRef = new AtomicReference<>(initialSessionId);

        AtomicReference<Long> convoPkRef = new AtomicReference<>(null);
        AtomicReference<Long> assistantMsgPkRef = new AtomicReference<>(null);

        AtomicReference<StringBuilder> bufRef = new AtomicReference<>(new StringBuilder());
        AtomicBoolean truncated = new AtomicBoolean(false);

        AtomicBoolean ctxInit = new AtomicBoolean(false);
        AtomicBoolean metaEmitted = new AtomicBoolean(false);

        // flag: apakah convo sudah ada sejak awal?
        AtomicBoolean convoExistedAtStart = new AtomicBoolean(false);

        // 1) load user (blocking JPA -> boundedElastic)
        Mono<User> userMono = Mono.fromCallable(() ->
                        userRepository.findById(userId)
                                .orElseThrow(() -> new StreamException("User not found: " + userId)))
                .subscribeOn(Schedulers.boundedElastic())
                .cache();

        // 2) check existing conversation if session_id provided
        Mono<Conversation> convoIfExistsMono = userMono.flatMap(user ->
                        Mono.fromCallable(() -> {
                                    if (initialSessionId != null && !initialSessionId.isBlank()) {
                                        return conversationRepository.findByUserAndSessionId(user, initialSessionId).orElse(null);
                                    }
                                    return null;
                                })
                                .subscribeOn(Schedulers.boundedElastic())
                )
                .cache();

        // 3) preSave user msg ONLY if convo exists
        Mono<Void> preSave = userMono.zipWith(convoIfExistsMono)
                .flatMap(tuple -> Mono.fromCallable(() -> {
                    Conversation convo = tuple.getT2();
                    if (convo != null) {
                        convoExistedAtStart.set(true);

                        sessionIdRef.set(convo.getSessionId());
                        convoPkRef.set(convo.getId());

                        persistence.saveUserMessage(convo, question);
                    }
                    return (Void) null;
                }).subscribeOn(Schedulers.boundedElastic()))
                .then();

        // 4) streaming upstream -> SSE
        Flux<ServerSentEvent<ChatEvent>> upstreamSse =
                upStreamClient.streamNdjsonLines(req)
                        .concatMap(line -> {
                            if (line == null) {
                                return Flux.empty();
                            }
                            String normalized = line.trim();
                            if (normalized.isEmpty() || normalized.startsWith("event:")) {
                                return Flux.empty();
                            }
                            if (normalized.startsWith("data:")) {
                                normalized = normalized.substring(5).trim();
                            }

                            final JsonNode root;
                            try {
                                root = om.readTree(normalized);
                            } catch (Exception e) {
                                return Flux.just(sse("error", ChatEvent.error(
                                        sessionIdRef.get(), convoPkRef.get(), assistantMsgPkRef.get(), runIdRef.get(),
                                        "Invalid upstream JSON: " + e.getMessage()
                                )));
                            }

                            String event = root.path("event").asText("");
                            String runId = root.path("run_id").asText(null);
                            if (runId == null || runId.isBlank()) {
                                runId = root.path("data").path("run_id").asText(null);
                            }
                            if (runId != null && !runId.isBlank()) runIdRef.compareAndSet(null, runId);

                            if ("metadata".equals(event)) {
                                String effectiveRunId = (runIdRef.get() != null && !runIdRef.get().isBlank())
                                        ? runIdRef.get()
                                        : "run-fallback-" + System.nanoTime();

                                Mono<Void> initMono = userMono.zipWith(convoIfExistsMono)
                                        .flatMap(tuple -> ensureContextOnce(
                                                ctxInit,
                                                tuple.getT1(),
                                                tuple.getT2(),
                                                convoExistedAtStart.get(),
                                                effectiveRunId,
                                                question,
                                                sessionIdRef,
                                                convoPkRef,
                                                assistantMsgPkRef
                                        ));

                                return initMono.thenMany(Flux.defer(() -> {
                                    if (metaEmitted.compareAndSet(false, true)) {
                                        return Flux.just(sse("metadata", ChatEvent.metadata(
                                                sessionIdRef.get(), convoPkRef.get(), assistantMsgPkRef.get(), runIdRef.get()
                                        )));
                                    }
                                    return Flux.empty();
                                }));
                            }

                            // upstream kamu utama: on_chat_model_stream
                            if (!"on_chat_model_stream".equals(event)) {
                                return Flux.empty();
                            }

                            String delta = root.path("data").path("chunk").path("content").asText("");
                            if (delta == null || delta.isBlank()) return Flux.empty();

                            String effectiveRunId = (runIdRef.get() != null && !runIdRef.get().isBlank())
                                    ? runIdRef.get()
                                    : "run-fallback-" + System.nanoTime();

                            // ensure context once, AFTER first chunk arrives (because run_id exists here)
                            Mono<Void> initMono = userMono.zipWith(convoIfExistsMono)
                                    .flatMap(tuple -> ensureContextOnce(
                                            ctxInit,
                                            tuple.getT1(),              // user
                                            tuple.getT2(),              // existing convo (nullable)
                                            convoExistedAtStart.get(),  // existed from pre-check?
                                            effectiveRunId,
                                            question,
                                            sessionIdRef,
                                            convoPkRef,
                                            assistantMsgPkRef
                                    ));

                            return initMono.thenMany(Flux.defer(() -> {

                                Flux<ServerSentEvent<ChatEvent>> meta = Flux.empty();
                                if (metaEmitted.compareAndSet(false, true)) {
                                    meta = Flux.just(sse("metadata", ChatEvent.metadata(
                                            sessionIdRef.get(), convoPkRef.get(), assistantMsgPkRef.get(), runIdRef.get()
                                    )));
                                }

                                // buffer append with limit
                                if (!truncated.get()) {
                                    StringBuilder sb = bufRef.get();
                                    int remaining = MAX_BUFFER_CHARS - sb.length();
                                    if (remaining > 0) {
                                        if (delta.length() <= remaining) sb.append(delta);
                                        else {
                                            sb.append(delta, 0, remaining);
                                            truncated.set(true);
                                        }
                                    } else truncated.set(true);
                                }

                                Flux<ServerSentEvent<ChatEvent>> chunk = Flux.just(
                                        sse("chunk", ChatEvent.chunk(
                                                sessionIdRef.get(), convoPkRef.get(), assistantMsgPkRef.get(), runIdRef.get(), delta
                                        ))
                                );

                                return Flux.concat(meta, chunk);
                            }));
                        })
                        .onErrorResume(err -> Flux.just(
                                sse("error", ChatEvent.error(
                                        sessionIdRef.get(), convoPkRef.get(), assistantMsgPkRef.get(), runIdRef.get(),
                                        err.getMessage()
                                ))
                        ));

        // 5) finalize assistant at end
        Flux<ServerSentEvent<ChatEvent>> finalize =
                Mono.fromCallable(() -> {
                            Long asstId = assistantMsgPkRef.get();
                            if (asstId != null) {
                                String finalText = bufRef.get().toString();
                                if (truncated.get()) finalText = finalText + "\n\n[TRUNCATED]";
                                persistence.finalizeAssistantMessage(asstId, finalText);
                            }
                            return sse("done", ChatEvent.done(
                                    sessionIdRef.get(), convoPkRef.get(), assistantMsgPkRef.get(), runIdRef.get()
                            ));
                        })
                        .subscribeOn(Schedulers.boundedElastic())
                        .flux();

        return preSave
                .thenMany(upstreamSse.concatWith(finalize))
                .doFinally(sig -> {
                    if (sig == reactor.core.publisher.SignalType.CANCEL) {
                        log.warn("Client disconnected. sessionId={}, runId={}", sessionIdRef.get(), runIdRef.get());
                    }
                });
    }

    /**
     * init context exactly once:
     * - decide sessionId final (prefer request.session_id, else run_id)
     * - ensure conversation exists
     * - if conversation did NOT exist at start, save user message now
     * - create assistant placeholder
     */
    private Mono<Void> ensureContextOnce(
            AtomicBoolean ctxInit,
            User user,
            Conversation existingConvoOrNull,
            boolean convoExistedAtStart,
            String runId,
            String question,
            AtomicReference<String> sessionIdRef,
            AtomicReference<Long> convoPkRef,
            AtomicReference<Long> assistantMsgPkRef
    ) {
        return Mono.fromCallable(() -> {
                    if (!ctxInit.compareAndSet(false, true)) {
                        return (Void) null;
                    }

                    // session final: prefer request.session_id, else run_id
                    String finalSessionId = (sessionIdRef.get() == null || sessionIdRef.get().isBlank())
                            ? runId
                            : sessionIdRef.get();
                    sessionIdRef.set(finalSessionId);

                    // if we already had convo (existing), use it; otherwise create/ensure
                    Conversation convo = existingConvoOrNull;
                    if (convo == null) {
                        convo = persistence.ensureConversation(user, finalSessionId, titleFromQuestion(question));
                    }

                    convoPkRef.set(convo.getId());

                    // Only save user message here if it was NOT saved earlier
                    // (saved earlier happens only when convo existed at start)
                    if (!convoExistedAtStart && existingConvoOrNull == null) {
                        persistence.saveUserMessage(convo, question);
                    }

                    Message placeholder = persistence.createAssistantPlaceholder(convo);
                    assistantMsgPkRef.set(placeholder.getId());

                    return (Void) null;
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    private String titleFromQuestion(String q) {
        if (q == null) return "New Chat";
        String s = q.strip();
        return s.length() <= 60 ? s : s.substring(0, 60);
    }

    private ServerSentEvent<ChatEvent> sse(String event, ChatEvent data) {
        return ServerSentEvent.<ChatEvent>builder().event(event).data(data).build();
    }
}
