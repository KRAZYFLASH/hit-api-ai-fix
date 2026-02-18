package com.example.hitapiai.service.impl;
import com.example.hitapiai.payload.response.ConversationDTO;
import com.example.hitapiai.repository.ConversationRepository;
import com.example.hitapiai.repository.MessageRepository;
import com.example.hitapiai.service.ChatService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ChatServiceImpl implements ChatService {

    private final ConversationRepository conversationRepository;
    private final MessageRepository messageRepository;

    @Override
    @Transactional
    public Mono<Void> deleteConversation(String conversationId) {
        return conversationRepository.findByConversationId(conversationId)
                .flatMap(conversationRepository::delete);
    }

    @Override
    @Transactional(readOnly = true)
    public Mono<ConversationDTO> getConversation(String conversationId) {
        return conversationRepository.findByConversationId(conversationId)
                .switchIfEmpty(Mono.error(new RuntimeException("Conversation not found: " + conversationId)))
                .flatMap(convo -> messageRepository.findByConversationIdOrderByCreatedAtAsc(convo.getConversationId())
                        .collectList()
                        .map(messages -> ConversationDTO.builder()
                                .conversation_id(convo.getConversationId())
                                .user_id(convo.getUserId())
                                .title(convo.getTitle())
                                .messages(messages.stream()
                                        .map(m -> ConversationDTO.MessageDTO.builder()
                                                .id(m.getId())
                                                .role(m.getRole())
                                                .content(m.getContentClob())
                                                .created_at(m.getCreatedAt())
                                                .build())
                                        .collect(Collectors.toList()))
                                .build())
                );
    }

    @Override
    @Transactional
    public Mono<Void> updateTitle(String conversationId, String newTitle) {
        return conversationRepository.findByConversationId(conversationId)
                .switchIfEmpty(Mono.error(new RuntimeException("Conversation not found: " + conversationId)))
                .flatMap(convo -> {
                    convo.setTitle(newTitle);
                    convo.setNew(false);
                    return conversationRepository.save(convo);
                })
                .then();
    }
}
