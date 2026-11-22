package com.defomo.marketdata.adapter.in.web;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/** 관리자 체결 입력용 WebSocket 엔드포인트를 등록한다. */
@Configuration
@EnableWebSocket
@Profile("demo")
@RequiredArgsConstructor
public class AdminTradeWebSocketConfiguration implements WebSocketConfigurer {

	private final AdminTradeWebSocketHandler adminTradeWebSocketHandler;

	@Override
	public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
		registry.addHandler(adminTradeWebSocketHandler, "/ws/admin/trades")
			.setAllowedOriginPatterns("*");
	}
}
