package com.example.hitapiai.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.mapping.Column;
import lombok.*;

import java.time.LocalDateTime;
import java.util.List;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Table("CONVERSATIONS")
public class Conversation implements Persistable<String> {

    @Id
    @Column("CONVERSATION_ID")
    private String conversationId;

    @Column("USER_ID")
    private String userId;

    @Column("TITLE")
    private String title;

    @Transient
    private List<Message> messages;

    @Transient
    private List<StreamEvent> streamEvents;

    @Column("CREATED_AT")
    private LocalDateTime createdAt;

    @Column("UPDATED_AT")
    private LocalDateTime updatedAt;

    @Transient
    private boolean isNew = true;

    @Override
    public String getId() {
        return conversationId;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    public void setNew(boolean isNew) {
        this.isNew = isNew;
    }
}
