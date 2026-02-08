package com.example.hitapiai.repository;

import com.example.hitapiai.model.User;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<User, String> {}
