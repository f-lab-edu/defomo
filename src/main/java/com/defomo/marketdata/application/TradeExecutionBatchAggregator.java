package com.defomo.marketdata.application;

import com.defomo.marketdata.application.port.out.TradeExecutionEventPublisher;
import com.defomo.marketdata.domain.model.TradeExecutionEvent;
import io.micrometer.core.instrument.Gauge;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 종목별 체결을 짧게 모아 가격 범위 이벤트로 만들고 발행자에게 전달한다. */
@Component
@Slf4j
public class TradeExecutionBatchAggregator {

	private final TradeExecutionEventPublisher eventPublisher;
	private final MeterRegistry meterRegistry;
	private final int maxTradesPerBatch;
	private final long windowNanos;
	private final AtomicLong pendingTrades = new AtomicLong();
	private final AtomicLong pendingBatches = new AtomicLong();
	/** 종목마다 현재 집계 중인 배치 하나를 보관한다. */
	private final Map<InstrumentKey, MutableBatch> batches = new ConcurrentHashMap<>();

	public TradeExecutionBatchAggregator(
		TradeExecutionEventPublisher eventPublisher,
		MeterRegistry meterRegistry,
		@Value("${defomo.market-data.max-trades-per-batch:500}") int maxTradesPerBatch,
		@Value("${defomo.market-data.batch-window-ms:100}") long batchWindowMillis) {
		if (maxTradesPerBatch < 1 || batchWindowMillis < 1) {
			throw new IllegalArgumentException("Batch window and maximum batch size must be positive");
		}
		this.eventPublisher = eventPublisher;
		this.meterRegistry = meterRegistry;
		this.maxTradesPerBatch = maxTradesPerBatch;
		this.windowNanos = batchWindowMillis * 1_000_000;
		// 집계 중인 체결 수
		Gauge.builder("defomo.marketdata.batches.pending.trades", pendingTrades, AtomicLong::get)
			.description("Trades currently held in in-memory aggregation batches")
			.register(meterRegistry);
		// 집계 중인 배치 수
		Gauge.builder("defomo.marketdata.batches.pending.count", pendingBatches, AtomicLong::get)
			.description("Number of in-memory aggregation batches not yet completed")
			.register(meterRegistry);
	}

	/**
	 * 종목별 배치에 체결 가격을 추가하고, 시간 또는 건수 제한에 도달하면 이벤트를 발행한다.
	 */
	public void accept(String market, String symbol, BigDecimal price) {
		meterRegistry.counter("defomo.marketdata.trades.received").increment();
		InstrumentKey key = new InstrumentKey(market, symbol);
		List<TradeExecutionEvent> completed = new ArrayList<>(2);
		log.debug("체결 데이터 입력: market={}, symbol={}, price={}", market, symbol, price);
		// 같은 종목의 배치 조회·수정·마감을 하나의 맵 작업으로 처리한다.
		batches.compute(key, (instrument, batch) -> {
			// 새 체결을 받았을 때 기존 배치가 이미 만료됐다면 먼저 마감한다.
			if (batch != null && System.nanoTime() - batch.createdAtNanos >= windowNanos) {
				log.debug("체결 배치 마감: 사유=새 체결 입력 전 집계 시간 초과, market={}, symbol={}, "
						+ "tradeCount={}, minPrice={}, maxPrice={}",
					instrument.market(), instrument.symbol(), batch.tradeCount, batch.minPrice, batch.maxPrice);
				completed.add(batch.toEvent(instrument));
				// 배치 처리가 완료되었으므로 모니터링 지표인 배치 수와 체결 데이터 수를 갱신한다.
				pendingTrades.addAndGet(-batch.tradeCount);
				pendingBatches.decrementAndGet();
				batch = null;
			}
			if (batch == null) {
				batch = new MutableBatch();
				// 모니터링 지표 (배치 수) 갱신
				pendingBatches.incrementAndGet();
			}
			// 같은 종목 배치의 가격 범위와 체결 건수를 갱신한다.
			batch.add(price);
			// 모니터링 지표 (집계된 체결 데이터 개수) 갱신
			pendingTrades.incrementAndGet();
			log.debug("체결 배치 집계 상태: market={}, symbol={}, tradeCount={}, minPrice={}, maxPrice={}, "
					+ "batchStartTime={}, batchEndTime={}",
				instrument.market(), instrument.symbol(), batch.tradeCount, batch.minPrice, batch.maxPrice,
				batch.batchStartTime, batch.batchEndTime);
			// 최대 건수에 도달하면 시간 윈도우가 끝나기 전에 배치를 마감한다.
			if (batch.tradeCount >= maxTradesPerBatch) {
				log.debug("체결 배치 마감: 사유=최대 체결 건수 도달, market={}, symbol={}, "
						+ "tradeCount={}, minPrice={}, maxPrice={}",
					instrument.market(), instrument.symbol(), batch.tradeCount, batch.minPrice, batch.maxPrice);
				completed.add(batch.toEvent(instrument));
				// 배치 처리가 완료되었으므로 모니터링 지표인 배치 수와 체결 데이터 수를 갱신한다.
				pendingTrades.addAndGet(-batch.tradeCount);
				pendingBatches.decrementAndGet();
				return null;
			}
			return batch;
		});
		// 완성된 배치를 Kafka로 발행한다.
		publishCompleted(completed);
	}

	/** 새 체결이 없어도 시간 제한이 지난 배치를 마감해 발행한다. */
	@Scheduled(fixedDelay = 10)
	public void flushExpiredBatches() {
		long now = System.nanoTime();
		List<TradeExecutionEvent> completed = new ArrayList<>();
		for (Map.Entry<InstrumentKey, MutableBatch> entry : batches.entrySet()) {
			batches.computeIfPresent(entry.getKey(), (key, batch) -> {
				if (now - batch.createdAtNanos >= windowNanos) {
					log.debug("체결 배치 마감: 사유=집계 시간 초과, market={}, symbol={}, "
							+ "tradeCount={}, minPrice={}, maxPrice={}",
						key.market(), key.symbol(), batch.tradeCount, batch.minPrice, batch.maxPrice);
					completed.add(batch.toEvent(key));
					// 배치 처리가 완료되었으므로 모니터링 지표인 배치 수와 체결 데이터 수를 갱신한다.
					pendingTrades.addAndGet(-batch.tradeCount);
					pendingBatches.decrementAndGet();
					return null;
				}
				return batch;
			});
		}
		// 완성된 배치를 Kafka로 발행한다.
		publishCompleted(completed);
	}

	private void publishCompleted(List<TradeExecutionEvent> completed) {
		completed.forEach(event -> {
			// 모니터링 지표 갱신: 집계를 마친 배치와 배치에 포함된 체결 데이터 누적 개수 증가
			meterRegistry.counter("defomo.marketdata.batches.completed").increment();
			meterRegistry.counter("defomo.marketdata.batch.trades.completed").increment(event.tradeCount());
			eventPublisher.publish(event);
		});
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
