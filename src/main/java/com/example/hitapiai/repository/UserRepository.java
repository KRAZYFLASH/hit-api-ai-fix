package com.example.hitapiai.repository;

import com.example.hitapiai.model.User;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface UserRepository extends ReactiveCrudRepository<User, String> {
}
