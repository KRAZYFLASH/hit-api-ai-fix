package com.example.hitapiai.repository;

import com.example.hitapiai.model.StreamEvent;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface StreamEventRepository extends ReactiveCrudRepository<StreamEvent, Long> {
}
