package com.example.hitapiai.configuration.webClient;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.HttpProtocol;

@Configuration
public class WebClientConfig {

    @Value("${upstream.base-url}")
    private String baseUrl;

    @Value("${upstream.api-key}")
    private String apiKey;

    @Value("${app.ssl.insecure:false}")
    private boolean insecureSsl;

    @Bean
    public WebClient hitApiWebClient() throws Exception {

        SslContextBuilder sslBuilder = SslContextBuilder.forClient();
        if (insecureSsl) {
            // DEV ONLY: trust all certs (jangan untuk production)
            sslBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        }
        SslContext sslContext = sslBuilder.build();

        HttpClient httpClient = HttpClient.create()
                .protocol(HttpProtocol.H2, HttpProtocol.HTTP11)
                .secure(ssl -> ssl.sslContext(sslContext));

        return WebClient.builder()
                .baseUrl(baseUrl) // jangan pakai double //
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .defaultHeader(HttpHeaders.ACCEPT, MediaType.TEXT_EVENT_STREAM_VALUE)
                .defaultHeader("X-API-Key", apiKey)
                .build();
    }
}
