package com.example.hitapiai.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.mapping.Column;
import lombok.*;

import java.time.LocalDateTime;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Table("MESSAGES")
public class Message implements Persistable<Long> {

    @Id
    @Column("ID")
    private Long id;

    @Column("CONVERSATION_ID")
    private String conversationId;

    @Column("ROLE")
    private String role; // "user" / "assistant"

    @Column("CONTENT_CLOB")
    private String contentClob;

    @Column("CREATED_AT")
    private LocalDateTime createdAt;

    @Transient
    private boolean isNew = true;

    @Override
    public boolean isNew() {
        return isNew || id == null;
    }

    public void setNew(boolean isNew) {
        this.isNew = isNew;
    }
}
