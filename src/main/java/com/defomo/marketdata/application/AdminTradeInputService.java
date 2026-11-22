package com.defomo.marketdata.application;

import com.defomo.marketdata.application.port.out.TradeExecutionEventPublisher;
import com.defomo.marketdata.domain.model.TradeExecutionEvent;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Locale;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/** 관리자 입력으로 받은 체결을 개별 이벤트로 발행한다. */
@Service
@RequiredArgsConstructor
public class AdminTradeInputService {

	private final TradeExecutionEventPublisher eventPublisher;
	private final MeterRegistry meterRegistry;

	public void addTrade(String market, String symbol, BigDecimal price) {
		String normalizedMarket = market.trim().toUpperCase(Locale.ROOT);
		String normalizedSymbol = symbol.trim().toUpperCase(Locale.ROOT);
		meterRegistry.counter("defomo.marketdata.trades.received").increment();
		Instant receivedAt = Instant.now();
		eventPublisher.publish(new TradeExecutionEvent(
			UUID.randomUUID().toString(), normalizedMarket, normalizedSymbol,
			price, price, 1, receivedAt, receivedAt));
	}
}
