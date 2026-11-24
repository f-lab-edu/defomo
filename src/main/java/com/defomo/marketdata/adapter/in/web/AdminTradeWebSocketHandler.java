package com.defomo.marketdata.adapter.in.web;

import com.defomo.marketdata.application.TradeExecutionInputService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/** Postman 등에서 체결 입력을 보내는 인바운드 WebSocket 어댑터. */
@Component
@Profile("demo")
@RequiredArgsConstructor
public class AdminTradeWebSocketHandler extends TextWebSocketHandler {

	private final ObjectMapper objectMapper;
	private final Validator validator;
	private final TradeExecutionInputService tradeExecutionInputService;

	@Override
	protected void handleTextMessage(WebSocketSession session, TextMessage message) throws IOException {
		AdminTradeRequest request;
		try {
			request = objectMapper.readValue(message.getPayload(), AdminTradeRequest.class);
		} catch (JsonProcessingException exception) {
			send(session, Map.of("status", "error", "message", "메시지를 체결 JSON으로 읽을 수 없습니다."));
			return;
		}

		List<String> errors = validator.validate(request).stream()
			.map(ConstraintViolation::getMessage)
			.sorted()
			.toList();
		if (!errors.isEmpty()) {
			send(session, Map.of("status", "error", "errors", errors));
			return;
		}

		Map<String, String> response = new HashMap<>(Map.of(
			"status", "accepted",
			"message", "체결 수신 및 검증이 완료됐습니다.",
			"market", request.market().trim().toUpperCase(Locale.ROOT),
			"symbol", request.symbol().trim().toUpperCase(Locale.ROOT)));
		if (request.tradeId() != null) {
			response.put("tradeId", request.tradeId());
		}
		send(session, response);
		tradeExecutionInputService.addTrade(request.market(), request.symbol(), request.price());
	}

	private void send(WebSocketSession session, Map<String, ?> response) throws IOException {
		session.sendMessage(new TextMessage(objectMapper.writeValueAsString(response)));
	}
}
