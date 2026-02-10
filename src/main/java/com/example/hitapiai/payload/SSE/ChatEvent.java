package com.example.hitapiai.payload.SSE;

import lombok.*;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
public class ChatEvent {
    private String type;                 // metadata/chunk/done/error
    private String conversationId;            // conversations.conversation_id (Primary Key)
    private Long assistantMessageId;     // messages.id (assistant)
    private String runId;                // upstream run_id
    private String delta;                // chunk text
    private String status;               // STREAMING/DONE/ERROR
    private String error;                // error message

    public static ChatEvent metadata(String conversationId, Long assistantMsgId, String runId) {
        return new ChatEvent("metadata", conversationId, assistantMsgId, runId, null, "STREAMING", null);
    }

    public static ChatEvent chunk(String conversationId, Long assistantMsgId, String runId, String delta) {
        return new ChatEvent("chunk", conversationId, assistantMsgId, runId, delta, "STREAMING", null);
    }

    public static ChatEvent done(String conversationId, Long assistantMsgId, String runId) {
        return new ChatEvent("done", conversationId, assistantMsgId, runId, null, "DONE", null);
    }

    public static ChatEvent error(String conversationId, Long assistantMsgId, String runId, String message) {
        return new ChatEvent("error", conversationId, assistantMsgId, runId, null, "ERROR", message);
    }
}
