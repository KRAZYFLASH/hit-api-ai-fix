package com.example.hitapiai.payload.response;

import com.example.hitapiai.model.Message;
import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ConversationDTO {

    private String conversation_id;
    private String user_id;
    private String title;
    private Message message;

}
