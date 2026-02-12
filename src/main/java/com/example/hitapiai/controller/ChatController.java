package com.example.hitapiai.controller;

import com.example.hitapiai.payload.response.ConversationDTO;
import com.example.hitapiai.service.ChatService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/chat")
public class ChatController {

    private final ChatService chatService;

    @DeleteMapping("/{conversationId}")
    public ResponseEntity<Void> deleteChat(@PathVariable String conversationId) {
        chatService.deleteConversation(conversationId);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/{conversationId}")
    public ResponseEntity<ConversationDTO> getChat(@PathVariable String conversationId) {
        return ResponseEntity.ok(chatService.getConversation(conversationId));
    }
}
