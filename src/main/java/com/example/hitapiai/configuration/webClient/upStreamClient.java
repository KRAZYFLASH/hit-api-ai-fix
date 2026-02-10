package com.example.hitapiai.configuration.webClient;

import com.example.hitapiai.payload.request.StreamEventRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.nio.charset.StandardCharsets;

@Component
@RequiredArgsConstructor
public class upStreamClient {

    private final WebClient hitApiWebClient;

    // Contoh: endpoint upstream: POST /stream dengan response NDJSON
    public Flux<String> streamNdjsonLines(StreamEventRequest req) {
        return hitApiWebClient.post()
                .uri("/stream_events")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(req)
                .exchangeToFlux(resp -> resp.bodyToFlux(DataBuffer.class))
                .transform(this::splitLines);
    }

    private Flux<String> splitLines(Flux<DataBuffer> body) {
        return Flux.create(sink -> {
            StringBuilder carry = new StringBuilder();
            body.doOnNext(buf -> {
                        String chunk = StandardCharsets.UTF_8.decode(buf.asByteBuffer()).toString();
                        DataBufferUtils.release(buf);
                        carry.append(chunk);
                        int idx;
                        while ((idx = carry.indexOf("\n")) >= 0) {
                            String line = carry.substring(0, idx);
                            if (line.endsWith("\r")) {
                                line = line.substring(0, line.length() - 1);
                            }
                            sink.next(line);
                            carry.delete(0, idx + 1);
                        }
                    })
                    .doOnError(sink::error)
                    .doOnComplete(() -> {
                        if (carry.length() > 0) {
                            String line = carry.toString();
                            if (line.endsWith("\r")) {
                                line = line.substring(0, line.length() - 1);
                            }
                            sink.next(line);
                        }
                        sink.complete();
                    })
                    .subscribe();
        });
    }
}
