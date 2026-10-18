package com.defomo.marketdata.application;

import com.defomo.marketdata.application.port.out.TradeExecutionEventPublisher;
import com.defomo.marketdata.domain.model.TradeExecutionEvent;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 종목별 체결을 짧게 모아 가격 범위 이벤트로 만들고 발행자에게 전달한다. */
@Component
public class TradeExecutionBatchAggregator {

	private final TradeExecutionEventPublisher eventPublisher;
	private final int maxTradesPerBatch;
	private final long windowNanos;
	/** 종목마다 현재 집계 중인 배치 하나를 보관한다. */
	private final Map<InstrumentKey, MutableBatch> batches = new HashMap<>();

	public TradeExecutionBatchAggregator(
		TradeExecutionEventPublisher eventPublisher,
		@Value("${defomo.market-data.max-trades-per-batch:500}") int maxTradesPerBatch,
		@Value("${defomo.market-data.batch-window-ms:100}") long batchWindowMillis) {
		if (maxTradesPerBatch < 1 || batchWindowMillis < 1) {
			throw new IllegalArgumentException("Batch window and maximum batch size must be positive");
		}
		this.eventPublisher = eventPublisher;
		this.maxTradesPerBatch = maxTradesPerBatch;
		this.windowNanos = batchWindowMillis * 1_000_000;
	}

	/**
	 * 종목별 배치에 체결 가격을 추가하고, 시간 또는 건수 제한에 도달하면 이벤트를 발행한다.
	 */
	public void accept(String market, String symbol, BigDecimal price) {
		InstrumentKey key = new InstrumentKey(market, symbol);
		List<TradeExecutionEvent> completed = new ArrayList<>(2);
		synchronized (batches) {
			MutableBatch batch = batches.get(key);
			// 새 체결을 받았을 때 기존 배치가 이미 만료됐다면 먼저 마감한다.
			if (batch != null && System.nanoTime() - batch.createdAtNanos >= windowNanos) {
				completed.add(batch.toEvent(key));
				batches.remove(key);
				batch = null;
			}
			if (batch == null) {
				batch = new MutableBatch();
				batches.put(key, batch);
			}
			// 같은 종목 배치의 가격 범위와 체결 건수를 갱신한다.
			batch.add(price);
			// 최대 건수에 도달하면 시간 윈도우가 끝나기 전에 배치를 마감한다.
			if (batch.tradeCount >= maxTradesPerBatch) {
				completed.add(batch.toEvent(key));
				batches.remove(key);
			}
		}

		// 체결 수신을 막지 않도록 잠금을 푼 뒤 Kafka로 보낸다.
		completed.forEach(eventPublisher::publish);
	}

	/** 새 체결이 없어도 시간 제한이 지난 배치를 마감해 발행한다. */
	@Scheduled(fixedDelay = 10)
	public void flushExpiredBatches() {
		long now = System.nanoTime();
		List<TradeExecutionEvent> completed = new ArrayList<>();
		synchronized (batches) {
			var iterator = batches.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry<InstrumentKey, MutableBatch> entry = iterator.next();
				if (now - entry.getValue().createdAtNanos >= windowNanos) {
					completed.add(entry.getValue().toEvent(entry.getKey()));
					iterator.remove();
				}
			}
		}
		// 체결 수신을 막지 않도록 잠금을 푼 뒤 Kafka로 발행한다.
		completed.forEach(eventPublisher::publish);
	}

	private record InstrumentKey(String market, String symbol) {
	}

	private static final class MutableBatch {
		/** 경과 시간 측정용 단조 시계 값. 시스템 시각이 바뀌어도 윈도우 판정에 영향을 받지 않는다. */
		private final long createdAtNanos = System.nanoTime();
		/** 이벤트에 기록할 배치 시작 시각. */
		private final Instant batchStartTime = Instant.now();
		/** 배치 안에서 알림 조건 판정에 사용하는 최저가와 최고가. */
		private BigDecimal minPrice;
		private BigDecimal maxPrice;
		/** 배치에 포함된 체결 수. */
		private int tradeCount;
		/** 이벤트에 기록할 마지막 체결 처리 시각. */
		private Instant batchEndTime;

		/** 가격 범위와 체결 수를 갱신한다. */
		private void add(BigDecimal price) {
			minPrice = minPrice == null ? price : minPrice.min(price);
			maxPrice = maxPrice == null ? price : maxPrice.max(price);
			tradeCount++;
			batchEndTime = Instant.now();
		}

		/** 종목별 집계 결과를 체결 이벤트로 변환한다. */
		private TradeExecutionEvent toEvent(InstrumentKey key) {
			return new TradeExecutionEvent(
				UUID.randomUUID().toString(), key.market(), key.symbol(),
				minPrice, maxPrice, tradeCount, batchStartTime, batchEndTime);
		}
	}
}
