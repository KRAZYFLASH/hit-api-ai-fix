package com.example.hitapiai.service;

import com.example.hitapiai.model.*;
import com.example.hitapiai.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class ReactiveChatPersistence {

    private final ConversationRepository conversationRepository;
    private final MessageRepository messageRepository;
    private final StreamEventRepository streamEventRepository;

    private static final String EMPTY_CLOB = " ";

    @Transactional
    public Mono<Conversation> ensureConversation(String userId, String conversationId, String titleIfNew) {
        return conversationRepository.findByUserIdAndConversationId(userId, conversationId)
                .map(convo -> {
                    convo.setNew(false);
                    return convo;
                })
                .switchIfEmpty(Mono.defer(() -> {
                    Conversation convo = new Conversation();
                    convo.setUserId(userId);
                    convo.setConversationId(conversationId);
                    convo.setTitle(titleIfNew != null ? titleIfNew : "New Chat");
                    convo.setCreatedAt(LocalDateTime.now());
                    convo.setUpdatedAt(LocalDateTime.now());
                    convo.setNew(true);
                    return conversationRepository.save(convo);
                }));
    }

    @Transactional
    public Mono<Message> saveUserMessage(String conversationId, String question) {
        Message m = new Message();
        m.setConversationId(conversationId);
        m.setRole("user");
        m.setContentClob(normalizeContent(question));
        m.setCreatedAt(LocalDateTime.now());
        m.setNew(true);
        return messageRepository.save(m);
    }

    @Transactional
    public Mono<Message> createAssistantPlaceholder(String conversationId) {
        Message m = new Message();
        m.setConversationId(conversationId);
        m.setRole("assistant");
        m.setContentClob(EMPTY_CLOB);
        m.setCreatedAt(LocalDateTime.now());
        m.setNew(true);
        return messageRepository.save(m);
    }

    @Transactional
    public Mono<Void> finalizeAssistantMessage(Long assistantMsgId, String finalText) {
        return messageRepository.findById(assistantMsgId)
                .flatMap(am -> {
                    am.setContentClob(normalizeContent(finalText));
                    am.setNew(false);
                    return messageRepository.save(am);
                })
                .then();
    }

    private String normalizeContent(String text) {
        return (text == null || text.isBlank()) ? EMPTY_CLOB : text;
    }

    @Transactional
    public Mono<Void> logStreamEvent(String conversationId, String runId, String eventType, String payloadJson) {
        StreamEvent e = new StreamEvent();
        e.setConversationId(conversationId);
        e.setRunId(runId);
        e.setEventType(eventType);
        e.setPayloadJsonClob(payloadJson == null ? "{}" : payloadJson);
        e.setCreatedAt(LocalDateTime.now());
        e.setNew(true);
        return streamEventRepository.save(e).then();
    }
}
