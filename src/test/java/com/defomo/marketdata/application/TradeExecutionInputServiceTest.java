package com.defomo.marketdata.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.defomo.marketdata.domain.model.TradeExecutionEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class TradeExecutionInputServiceTest {
	@Test
	void publishesEachTradeBeforeReturningWithoutWaitingForMoreTrades() {
		var events = new ConcurrentLinkedQueue<TradeExecutionEvent>();
		var registry = new SimpleMeterRegistry();
		var service = new TradeExecutionInputService(events::add, registry);

		service.addTrade(" kr ", " load0000 ", new BigDecimal("100.25"));

		assertThat(events).hasSize(1);
		var event = events.element();
		assertThat(event.market()).isEqualTo("KR");
		assertThat(event.symbol()).isEqualTo("LOAD0000");
		assertThat(event.tradeCount()).isEqualTo(1);
		assertThat(event.minPrice()).isEqualByComparingTo("100.25");
		assertThat(event.maxPrice()).isEqualTo(event.minPrice());
		assertThat(event.batchEndTime()).isEqualTo(event.batchStartTime());
		assertThat(registry.counter("defomo.marketdata.trades.received").count()).isEqualTo(1);
	}

	@Test
	void concurrentTradesForTheSameSymbolRemainSeparateEvents() {
		var events = new ConcurrentLinkedQueue<TradeExecutionEvent>();
		var registry = new SimpleMeterRegistry();
		var service = new TradeExecutionInputService(events::add, registry);

		IntStream.range(0, 2000).parallel()
			.forEach(i -> service.addTrade("US", "LOAD0000", BigDecimal.valueOf(i)));

		assertThat(events).hasSize(2000);
		assertThat(events).allSatisfy(event -> {
			assertThat(event.tradeCount()).isEqualTo(1);
			assertThat(event.minPrice()).isEqualTo(event.maxPrice());
		});
		assertThat(events.stream().map(TradeExecutionEvent::eventId).distinct().count()).isEqualTo(2000);
		assertThat(events.stream().map(TradeExecutionEvent::minPrice).distinct().count()).isEqualTo(2000);
		assertThat(registry.counter("defomo.marketdata.trades.received").count()).isEqualTo(2000);
	}
}
