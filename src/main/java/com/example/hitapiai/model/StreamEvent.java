package com.example.hitapiai.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Entity
@Table(
        name = "stream_events",
        indexes = {
                @Index(name = "idx_stream_events_run_id", columnList = "run_id"),
                @Index(name = "idx_stream_events_convo_created", columnList = "conversation_id, created_at"),
                @Index(name = "idx_stream_events_type", columnList = "event_type")
        }
)
public class StreamEvent {

    @Id
    @SequenceGenerator(
            name = "stream_events_seq_gen",
            sequenceName = "stream_events_seq",
            allocationSize = 1
    )
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "stream_events_seq_gen")
    @Column(name = "id", nullable = false)
    private Long id;

    // nullable FK
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "conversation_id", foreignKey = @ForeignKey(name = "fk_stream_events_conversation"))
    private Conversation conversation;

    @Column(name = "run_id", length = 120)
    private String runId; // nullable

    @Column(name = "event_type", length = 120)
    private String eventType; // nullable

    @Lob
    @Column(name = "payload_json_clob", nullable = false, columnDefinition = "CLOB")
    private String payloadJsonClob;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
}
