package com.example.hitapiai.repository;

import com.example.hitapiai.model.Message;
import org.springframework.data.jpa.repository.JpaRepository;

public interface MessageRepository extends JpaRepository<Message, Long> {}
