package com.example.hitapiai.service;

import com.example.hitapiai.model.*;
import com.example.hitapiai.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class BlockingChatPersistence {

    private final ConversationRepository conversationRepository;
    private final MessageRepository messageRepository;
    private final UserSessionRepository userSessionRepository;
    private final StreamEventRepository streamEventRepository;

    private static final String EMPTY_CLOB = " ";

    @Transactional
    public Conversation ensureConversation(User user, String sessionId, String titleIfNew) {
        Conversation convo = conversationRepository.findByUserAndSessionId(user, sessionId).orElse(null);
        if (convo == null) {
            convo = new Conversation();
            convo.setUser(user);
            convo.setSessionId(sessionId);
            convo.setTitle(titleIfNew != null ? titleIfNew : "New Chat");
            convo = conversationRepository.save(convo);
        }
        upsertUserSession(user, sessionId);
        return convo;
    }

    @Transactional
    public void upsertUserSession(User user, String sessionId) {
        UserSession us = userSessionRepository.findBySessionId(sessionId).orElse(null);
        if (us == null) {
            us = new UserSession();
            us.setSessionId(sessionId);
            us.setUser(user);
            us.setCreatedAt(LocalDateTime.now());
        }
        userSessionRepository.save(us);
    }

    @Transactional
    public Message saveUserMessage(Conversation convo, String question) {
        Message m = new Message();
        m.setConversation(convo);
        m.setRole("user");
        m.setContentClob(normalizeContent(question));
        return messageRepository.save(m);
    }

    @Transactional
    public Message createAssistantPlaceholder(Conversation convo) {
        Message m = new Message();
        m.setConversation(convo);
        m.setRole("assistant");
        m.setContentClob(EMPTY_CLOB);
        return messageRepository.save(m);
    }

    @Transactional
    public void finalizeAssistantMessage(Long assistantMsgId, String finalText) {
        Message am = messageRepository.findById(assistantMsgId).orElseThrow();
        am.setContentClob(normalizeContent(finalText));
        messageRepository.save(am);
    }

    private String normalizeContent(String text) {
        return (text == null || text.isBlank()) ? EMPTY_CLOB : text;
    }

    @Transactional
    public void logStreamEvent(Conversation convoOrNull, String runId, String eventType, String payloadJson) {
        StreamEvent e = new StreamEvent();
        e.setConversation(convoOrNull);
        e.setRunId(runId);
        e.setEventType(eventType);
        e.setPayloadJsonClob(payloadJson == null ? "{}" : payloadJson);
        streamEventRepository.save(e);
    }
}
