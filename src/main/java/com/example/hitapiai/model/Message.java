package com.example.hitapiai.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Entity
@Table(
        name = "messages",
        indexes = {
                @Index(name = "idx_messages_conversation_created", columnList = "conversation_id, created_at"),
                @Index(name = "idx_messages_role", columnList = "role")
        }
)
public class Message {

    @Id
    @SequenceGenerator(
            name = "messages_seq_gen",
            sequenceName = "messages_seq",
            allocationSize = 1
    )
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "messages_seq_gen")
    @Column(name = "id", nullable = false)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "conversation_id", nullable = false, foreignKey = @ForeignKey(name = "fk_messages_conversation"))
    private Conversation conversation;

    @Column(name = "role", nullable = false, length = 30)
    private String role; // "user" / "assistant"

    @Lob
    @Column(name = "content_clob", nullable = false, columnDefinition = "CLOB")
    private String contentClob;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
}
