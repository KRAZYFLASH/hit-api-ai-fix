package com.example.hitapiai.service;

import com.example.hitapiai.payload.response.ConversationDTO;
import reactor.core.publisher.Mono;

public interface ChatService {

    Mono<Void> deleteConversation(String conversationId);

    Mono<ConversationDTO> getConversation(String conversationId);

    Mono<Void> updateTitle(String conversationId, String newTitle);
}
