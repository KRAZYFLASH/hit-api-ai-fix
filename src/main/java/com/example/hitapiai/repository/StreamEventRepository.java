package com.example.hitapiai.repository;

import com.example.hitapiai.model.StreamEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface StreamEventRepository extends JpaRepository<StreamEvent, Long> {}
