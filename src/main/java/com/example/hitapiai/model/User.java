package com.example.hitapiai.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.mapping.Column;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Table("USERS")
public class User implements Persistable<String> {

    @Id
    @Column("ID")
    private String id;

    @Column("USERNAME")
    private String username;

    @Column("EMAIL")
    private String email; // nullable

    @Column("PASSWORD")
    private String password;

    @Column("CREATED_AT")
    private LocalDateTime createdAt;

    @Column("LAST_LOGIN")
    private LocalDateTime lastLogin;

    @Transient
    private boolean isNew = true;

    @Override
    public boolean isNew() {
        return isNew;
    }

    public void setNew(boolean isNew) {
        this.isNew = isNew;
    }

    public void initializeId() {
        if (id == null || id.isBlank()) {
            id = UUID.randomUUID().toString();
        }
        if (createdAt == null) {
            createdAt = LocalDateTime.now();
        }
    }
}
