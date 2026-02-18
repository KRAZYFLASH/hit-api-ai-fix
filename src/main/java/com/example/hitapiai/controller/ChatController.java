package com.example.hitapiai.controller;

import com.example.hitapiai.payload.request.UpdateTitleRequest;
import com.example.hitapiai.payload.response.ConversationDTO;
import com.example.hitapiai.service.ChatService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/chat")
public class ChatController {

    private final ChatService chatService;

    @DeleteMapping("/{conversationId}")
    public Mono<Void> deleteChat(@PathVariable String conversationId) {
        return chatService.deleteConversation(conversationId);
    }

    @GetMapping("/{conversationId}")
    public Mono<ConversationDTO> getChat(@PathVariable String conversationId) {
        return chatService.getConversation(conversationId);
    }

    @PutMapping("/{conversationId}/title")
    public Mono<Void> updateTitle(@PathVariable String conversationId, @RequestBody UpdateTitleRequest request) {
        return chatService.updateTitle(conversationId, request.getTitle());
    }
}
