package com.example.hitapiai.service.impl;
import com.example.hitapiai.model.Conversation;
import com.example.hitapiai.payload.response.ConversationDTO;
import com.example.hitapiai.repository.ConversationRepository;
import com.example.hitapiai.service.ChatService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ChatServiceImpl implements ChatService {

    private final ConversationRepository conversationRepository;

    @Override
    @Transactional
    public void deleteConversation(String conversationId) {
        conversationRepository.findByConversationId(conversationId).ifPresent(conversationRepository::delete);
    }

    @Override
    @Transactional(readOnly = true)
    public ConversationDTO getConversation(String conversationId) {
        Conversation convo = conversationRepository.findByConversationId(conversationId)
                .orElseThrow(() -> new RuntimeException("Conversation not found: " + conversationId));

        return ConversationDTO.builder()
                .conversation_id(convo.getConversationId())
                .user_id(convo.getUser().getId())
                .title(convo.getTitle())
                .messages(convo.getMessages().stream()
                        .map(m -> ConversationDTO.MessageDTO.builder()
                                .id(m.getId())
                                .role(m.getRole())
                                .content(m.getContentClob())
                                .created_at(m.getCreatedAt())
                                .build())
                        .collect(Collectors.toList()))
                .build();
    }
}
