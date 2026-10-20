package com.defomo.marketdata.application;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.defomo.marketdata.application.port.out.TradeExecutionEventPublisher;
import com.defomo.marketdata.domain.model.TradeExecutionEvent;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.RepeatedTest;

class TradeExecutionBatchAggregatorConcurrencyTest {

	private static final System.Logger LOGGER = System.getLogger(TradeExecutionBatchAggregatorConcurrencyTest.class.getName());
	private static final int PRODUCER_COUNT = 4;
	private static final int TRADES_PER_PRODUCER = 2_500;
	private static final int TOTAL_TRADES = PRODUCER_COUNT * TRADES_PER_PRODUCER;

	@RepeatedTest(10)
	void publishesAllTradesWhenAcceptAndFlushRunConcurrently() throws Exception {
		ConcurrentLinkedQueue<TradeExecutionEvent> publishedEvents = new ConcurrentLinkedQueue<>();
		TradeExecutionEventPublisher publisher = publishedEvents::add;
		TradeExecutionBatchAggregator aggregator = new TradeExecutionBatchAggregator(
			publisher,
			TOTAL_TRADES + 1,
			1);

		ExecutorService executor = Executors.newFixedThreadPool(PRODUCER_COUNT + 1);
		CountDownLatch ready = new CountDownLatch(PRODUCER_COUNT + 1);
		CountDownLatch start = new CountDownLatch(1);
		AtomicBoolean producersFinished = new AtomicBoolean();
		AtomicInteger exceptionCount = new AtomicInteger();
		AtomicReference<RuntimeException> firstException = new AtomicReference<>();
	List<java.util.concurrent.Future<?>> producerTasks = new ArrayList<>();

		try {
			for (int producer = 0; producer < PRODUCER_COUNT; producer++) {
				int producerNumber = producer;
				producerTasks.add(executor.submit(() -> {
					ready.countDown();
					awaitStart(start, exceptionCount, firstException);
					for (int index = 0; index < TRADES_PER_PRODUCER; index++) {
						int price = producerNumber * TRADES_PER_PRODUCER + index + 1;
						try {
							aggregator.accept("KR", "TEST", BigDecimal.valueOf(price));
						} catch (RuntimeException exception) {
							recordException(exception, exceptionCount, firstException);
						}
					}
					return null;
				}));
			}

			java.util.concurrent.Future<?> flusherTask = executor.submit(() -> {
				ready.countDown();
				awaitStart(start, exceptionCount, firstException);
				while (!producersFinished.get()) {
					try {
						aggregator.flushExpiredBatches();
					} catch (RuntimeException exception) {
						recordException(exception, exceptionCount, firstException);
					}
					Thread.yield();
				}
				return null;
			});

			if (!ready.await(5, TimeUnit.SECONDS)) {
				throw new AssertionError("작업 스레드가 준비 시간 안에 시작되지 않았습니다.");
			}
			start.countDown();
			for (var producerTask : producerTasks) {
				producerTask.get(30, TimeUnit.SECONDS);
			}
			producersFinished.set(true);
			flusherTask.get(30, TimeUnit.SECONDS);

			// 마지막 체결로 만들어진 배치도 시간 제한이 지난 뒤 발행되도록 기다린다.
			Thread.sleep(Duration.ofMillis(10));
			try {
				aggregator.flushExpiredBatches();
			} catch (RuntimeException exception) {
				recordException(exception, exceptionCount, firstException);
			}

			List<TradeExecutionEvent> events = new ArrayList<>(publishedEvents);
			long publishedTradeCount = events.stream().mapToLong(TradeExecutionEvent::tradeCount).sum();
			BigDecimal minPrice = events.stream()
				.map(TradeExecutionEvent::minPrice)
				.min(BigDecimal::compareTo)
				.orElse(null);
			BigDecimal maxPrice = events.stream()
				.map(TradeExecutionEvent::maxPrice)
				.max(BigDecimal::compareTo)
				.orElse(null);

			LOGGER.log(System.Logger.Level.INFO,
				"집계 검증 결과: 입력 체결 수={0}, 이벤트 tradeCount 합={1}, 최저가={2}, 최고가={3}, 예외 수={4}",
				TOTAL_TRADES, publishedTradeCount, minPrice, maxPrice, exceptionCount.get());
			if (exceptionCount.get() > 0) {
				LOGGER.log(System.Logger.Level.WARNING,
					"동시성 처리 중 발생한 첫 번째 예외", firstException.get());
			}
		} finally {
			producersFinished.set(true);
			executor.shutdownNow();
			executor.awaitTermination(5, TimeUnit.SECONDS);
		}
	}

	private static void awaitStart(
		CountDownLatch start,
		AtomicInteger exceptionCount,
		AtomicReference<RuntimeException> firstException) {
		try {
			start.await();
		} catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			recordException(new RuntimeException("테스트 작업이 시작 대기 중 중단됐습니다.", exception),
				exceptionCount, firstException);
		}
	}

	private static void recordException(
		RuntimeException exception,
		AtomicInteger exceptionCount,
		AtomicReference<RuntimeException> firstException) {
		exceptionCount.incrementAndGet();
		firstException.compareAndSet(null, exception);
	}
}
