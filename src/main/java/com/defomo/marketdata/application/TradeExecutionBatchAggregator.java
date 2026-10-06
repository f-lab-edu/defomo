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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 체결을 종목별·샤드별로 짧게 모아 가격 범위 이벤트로 발행한다. */
@Component
public class TradeExecutionBatchAggregator {

	private final TradeExecutionEventPublisher eventPublisher;
	private final int shardCount;
	private final int maxTradesPerBatch;
	private final long windowNanos;
	private final Map<String, AtomicInteger> nextShardByInstrument = new ConcurrentHashMap<>();
	private final Map<ShardKey, MutableBatch> batches = new HashMap<>();

	public TradeExecutionBatchAggregator(
		TradeExecutionEventPublisher eventPublisher,
		@Value("${defomo.market-data.shard-count:6}") int shardCount,
		@Value("${defomo.market-data.max-trades-per-batch:500}") int maxTradesPerBatch,
		@Value("${defomo.market-data.batch-window-ms:100}") long batchWindowMillis) {
		if (shardCount < 1 || maxTradesPerBatch < 1 || batchWindowMillis < 1) {
			throw new IllegalArgumentException("Batch window, maximum batch size, and shard count must be positive");
		}
		this.eventPublisher = eventPublisher;
		this.shardCount = shardCount;
		this.maxTradesPerBatch = maxTradesPerBatch;
		this.windowNanos = batchWindowMillis * 1_000_000;
	}

	/** 체결 하나를 종목의 다음 샤드에 배정하고 조건에 도달한 Batch를 발행한다. */
	public void accept(String market, String symbol, BigDecimal price) {
		String instrument = market + ":" + symbol;
		int shard = Math.floorMod(
			nextShardByInstrument.computeIfAbsent(instrument, ignored -> new AtomicInteger()).getAndIncrement(),
			shardCount);
		List<TradeExecutionEvent> completed = new ArrayList<>(2);
		synchronized (batches) {
			ShardKey key = new ShardKey(market, symbol, shard);
			MutableBatch batch = batches.get(key);
			if (batch != null && System.nanoTime() - batch.createdAtNanos >= windowNanos) {
				completed.add(batch.toEvent(key));
				batches.remove(key);
				batch = null;
			}
			if (batch == null) {
				batch = new MutableBatch();
				batches.put(key, batch);
			}
			batch.add(price);
			if (batch.tradeCount >= maxTradesPerBatch) {
				completed.add(batch.toEvent(key));
				batches.remove(key);
			}
		}
		completed.forEach(eventPublisher::publish);
	}

	/** 윈도우 시간이 지난 Batch를 발행한다. */
	@Scheduled(fixedDelay = 10)
	public void flushExpiredBatches() {
		long now = System.nanoTime();
		List<TradeExecutionEvent> completed = new ArrayList<>();
		synchronized (batches) {
			var iterator = batches.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry<ShardKey, MutableBatch> entry = iterator.next();
				if (now - entry.getValue().createdAtNanos >= windowNanos) {
					completed.add(entry.getValue().toEvent(entry.getKey()));
					iterator.remove();
				}
			}
		}
		completed.forEach(eventPublisher::publish);
	}

	private record ShardKey(String market, String symbol, int shard) {
	}

	private static final class MutableBatch {
		private final long createdAtNanos = System.nanoTime();
		private final Instant batchStartTime = Instant.now();
		private BigDecimal minPrice;
		private BigDecimal maxPrice;
		private int tradeCount;
		private Instant batchEndTime;

		private void add(BigDecimal price) {
			minPrice = minPrice == null ? price : minPrice.min(price);
			maxPrice = maxPrice == null ? price : maxPrice.max(price);
			tradeCount++;
			batchEndTime = Instant.now();
		}

		private TradeExecutionEvent toEvent(ShardKey key) {
			return new TradeExecutionEvent(
				UUID.randomUUID().toString(), key.market(), key.symbol(), key.shard(),
				minPrice, maxPrice, tradeCount, batchStartTime, batchEndTime);
		}
	}
}
