package com.example.hitapiai.service;

import com.example.hitapiai.payload.SSE.ChatEvent;
import com.example.hitapiai.payload.request.StreamEventRequest;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;

public interface StreamService {
    Flux<ServerSentEvent<ChatEvent>> streamLive(StreamEventRequest req);
}
