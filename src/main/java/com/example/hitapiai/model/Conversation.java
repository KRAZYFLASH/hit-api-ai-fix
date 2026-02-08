package com.example.hitapiai.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Entity
@Table(
        name = "conversations",
        uniqueConstraints = {
                @UniqueConstraint(name = "uk_conversations_user_session", columnNames = {"user_id", "session_id"})
        },
        indexes = {
                @Index(name = "idx_conversations_user", columnList = "user_id"),
                @Index(name = "idx_conversations_session", columnList = "session_id"),
                @Index(name = "idx_conversations_updated", columnList = "updated_at")
        }
)
public class Conversation {

    @Id
    @SequenceGenerator(
            name = "conversations_seq_gen",
            sequenceName = "conversations_seq",
            allocationSize = 1
    )
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "conversations_seq_gen")
    @Column(name = "id", nullable = false)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "user_id", nullable = false, foreignKey = @ForeignKey(name = "fk_conversations_user"))
    private User user;

    @Column(name = "title", nullable = false, length = 200)
    private String title;

    @Column(name = "session_id", nullable = false, length = 100)
    private String sessionId;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
}
