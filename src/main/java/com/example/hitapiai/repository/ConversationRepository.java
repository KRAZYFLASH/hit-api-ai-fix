package com.example.hitapiai.repository;

import com.example.hitapiai.model.Conversation;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

public interface ConversationRepository extends ReactiveCrudRepository<Conversation, String> {
    
    @Query("SELECT * FROM CONVERSATIONS WHERE USER_ID = :userId AND CONVERSATION_ID = :conversationId")
    Mono<Conversation> findByUserIdAndConversationId(String userId, String conversationId);

    Mono<Conversation> findByConversationId(String conversationId);
}
