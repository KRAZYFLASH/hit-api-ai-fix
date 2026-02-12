package com.example.hitapiai.service;

import com.example.hitapiai.payload.response.ConversationDTO;

public interface ChatService {

    void deleteConversation(String conversationId);

    ConversationDTO getConversation(String conversationId);
}
