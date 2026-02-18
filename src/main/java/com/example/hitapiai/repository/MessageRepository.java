package com.example.hitapiai.repository;

import com.example.hitapiai.model.Message;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface MessageRepository extends ReactiveCrudRepository<Message, Long> {
    Flux<Message> findByConversationIdOrderByCreatedAtAsc(String conversationId);
}
