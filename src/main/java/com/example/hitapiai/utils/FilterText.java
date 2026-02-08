package com.example.hitapiai.utils;

import com.example.hitapiai.payload.request.StreamEventRequest;
import org.springframework.stereotype.Component;

@Component
public class FilterText {

    public String extractUserText(StreamEventRequest req) {
        if (req == null || req.getInput() == null || req.getInput().getMessages() == null) return "";
        if (req.getInput().getMessages().isEmpty()) return "";

        var msg = req.getInput().getMessages().get(0);
        if (msg == null || msg.getContent() == null || msg.getContent().isEmpty()) return "";

        var c = msg.getContent().get(0);
        return c == null || c.getText() == null ? "" : c.getText();
    }
}
