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
@Table("STREAM_EVENTS")
public class StreamEvent implements Persistable<Long> {

    @Id
    @Column("ID")
    private Long id;

    @Column("CONVERSATION_ID")
    private String conversationId;

    @Column("RUN_ID")
    private String runId; // nullable

    @Column("EVENT_TYPE")
    private String eventType; // nullable

    @Column("PAYLOAD_JSON_CLOB")
    private String payloadJsonClob;

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
