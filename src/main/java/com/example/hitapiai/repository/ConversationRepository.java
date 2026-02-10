package com.example.hitapiai.repository;

import com.example.hitapiai.model.Conversation;
import com.example.hitapiai.model.User;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface ConversationRepository extends JpaRepository<Conversation, String> {
    Optional<Conversation> findByUserAndConversationId(User user, String conversationId);
    Optional<Conversation> findByConversationId(String conversationId);
}
