package com.example.hitapiai.payload.response;

import lombok.*;

import java.time.LocalDateTime;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ConversationDTO {

    private String conversation_id;
    private String user_id;
    private String title;
    private List<MessageDTO> messages;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class MessageDTO {
        private Long id;
        private String role;
        private String content;
        private LocalDateTime created_at;
    }
}
