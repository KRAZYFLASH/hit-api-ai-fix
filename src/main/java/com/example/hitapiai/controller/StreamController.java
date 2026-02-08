package com.example.hitapiai.controller;

import com.example.hitapiai.payload.SSE.ChatEvent;
import com.example.hitapiai.payload.request.StreamEventRequest;
import com.example.hitapiai.service.StreamService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api")
public class StreamController {

    private final StreamService streamService;

    @PostMapping(value = "/stream/live")
    public Flux<ServerSentEvent<ChatEvent>> streamLive(@RequestBody StreamEventRequest req) {
        return streamService.streamLive(req);
    }
}
