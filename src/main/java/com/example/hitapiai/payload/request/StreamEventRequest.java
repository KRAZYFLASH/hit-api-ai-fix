package com.example.hitapiai.payload.request;

import lombok.*;

import java.util.List;
import java.util.Map;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
public class StreamEventRequest {

    private Input input;

    @Getter @Setter
    @NoArgsConstructor @AllArgsConstructor
    public static class Input {
        private List<MessageDto> messages;
        private String user_id;
        private String session_id;
        private String divisi_type;
    }

    @Getter @Setter
    @NoArgsConstructor @AllArgsConstructor
    public static class MessageDto {
        private List<ContentDto> content;          // [{type:"text", text:"..."}]
        private Map<String, Object> additional_kwargs;
        private Map<String, Object> response_metadata;
        private String type;                       // "human"
        private String name;
        private String id;
        private boolean example;
    }

    @Getter @Setter
    @NoArgsConstructor @AllArgsConstructor
    public static class ContentDto {
        private String type; // "text"
        private String text;
    }
}
