package com.example.hitapiai.payload.SSE;

import lombok.*;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
public class ChatEvent {
    private String type;                 // metadata/chunk/done/error
    private String sessionId;            // conversations.session_id
    private Long conversationId;         // conversations.id
    private Long assistantMessageId;     // messages.id (assistant)
    private String runId;                // upstream run_id
    private String delta;                // chunk text
    private String status;               // STREAMING/DONE/ERROR
    private String error;                // error message

    public static ChatEvent metadata(String sessionId, Long convoId, Long assistantMsgId, String runId) {
        return new ChatEvent("metadata", sessionId, convoId, assistantMsgId, runId, null, "STREAMING", null);
    }

    public static ChatEvent chunk(String sessionId, Long convoId, Long assistantMsgId, String runId, String delta) {
        return new ChatEvent("chunk", sessionId, convoId, assistantMsgId, runId, delta, "STREAMING", null);
    }

    public static ChatEvent done(String sessionId, Long convoId, Long assistantMsgId, String runId) {
        return new ChatEvent("done", sessionId, convoId, assistantMsgId, runId, null, "DONE", null);
    }

    public static ChatEvent error(String sessionId, Long convoId, Long assistantMsgId, String runId, String message) {
        return new ChatEvent("error", sessionId, convoId, assistantMsgId, runId, null, "ERROR", message);
    }
}
