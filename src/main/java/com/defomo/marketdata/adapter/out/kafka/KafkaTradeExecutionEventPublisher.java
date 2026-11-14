package com.defomo.marketdata.adapter.out.kafka;

import com.defomo.marketdata.application.port.out.TradeExecutionEventPublisher;
import com.defomo.marketdata.domain.model.TradeExecutionEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** 집계된 체결 이벤트를 Kafka에 발행한다. */
@Component
@Slf4j
public class KafkaTradeExecutionEventPublisher implements TradeExecutionEventPublisher {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ObjectMapper objectMapper;
	private final String topicName;
	private final MeterRegistry meterRegistry;
	private final AtomicLong publishesInFlight = new AtomicLong();

	public KafkaTradeExecutionEventPublisher(
		KafkaTemplate<String, String> kafkaTemplate,
		ObjectMapper objectMapper,
		MeterRegistry meterRegistry,
		@Value("${defomo.kafka.trade-executions-topic}") String topicName) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
		this.meterRegistry = meterRegistry;
		this.topicName = topicName;
		// Kafka 이벤트 발행을 요청한 데이터로 그 중에서 아직 kafka로부터 수신 완료를 받지 않은 수
		Gauge.builder("defomo.kafka.trade.events.publish.in.flight", publishesInFlight, AtomicLong::get)
			.description("Kafka publish requests awaiting broker acknowledgement")
			.register(meterRegistry);
	}

	@Override
	public void publish(TradeExecutionEvent event) {
		String payload;
		try {
			payload = objectMapper.writeValueAsString(event);
		} catch (JsonProcessingException exception) {
			// 전송 전에 JSON 변환이 실패해도 발행 실패로 센다.
			recordFailure(event);
			log.error("Kafka 집계 이벤트 JSON 변환 실패: market={}, symbol={}", event.market(), event.symbol(), exception);
			return;
		}

		// 파티션과 키를 지정하지 않아 Kafka의 RoundRobinPartitioner가 파티션을 고르게 선택하게 한다.
		// 종목별 전체 순서보다 모든 파티션에 이벤트를 분산하는 것을 우선한다.
		// 전송 요청 때 늘리고 완료 콜백이나 예외가 발생하면 줄인다.
		publishesInFlight.incrementAndGet();
		try {
			kafkaTemplate.send(topicName, null, payload)
				.whenComplete((result, error) -> {
					try {
						if (error != null) {
							// 모니터링 지표 갱신: 발행한 이벤트가 Kafka 수신을 실패한 경우
							recordFailure(event);
							log.error("Kafka 집계 이벤트 발행 실패: eventId={}, market={}, symbol={}, tradeCount={}, minPrice={}, maxPrice={}",
								event.eventId(), event.market(), event.symbol(), event.tradeCount(),
								event.minPrice(), event.maxPrice(), error);
							return;
						}
						// 모니터링 지표 갱신: 발행한 이벤트가 Kafka 정상적으로 수신을 성공 경우
						meterRegistry.counter("defomo.kafka.trade.events.publish.successes", "market", event.market())
							.increment();
						log.debug("Kafka 집계 이벤트 발행 완료: topic={}, partition={}, offset={}, eventId={}, market={}, "
								+ "symbol={}, tradeCount={}, minPrice={}, maxPrice={}",
							result.getRecordMetadata().topic(),
							result.getRecordMetadata().partition(),
							result.getRecordMetadata().offset(),
							event.eventId(), event.market(), event.symbol(), event.tradeCount(),
							event.minPrice(), event.maxPrice());
					} finally {
						// 모니터링 지표 갱신: 발행한 이벤트 건수 중 아직 kafka 수신이 완료되지 않은 이벤트 개수
						publishesInFlight.decrementAndGet();
					}
				});
		} catch (RuntimeException exception) {
			// 모니터링 지표 갱신: 발행한 이벤트 건수 중 아직 kafka 수신이 완료되지 않은 이벤트 개수
			publishesInFlight.decrementAndGet();
			// 모니터링 지표 갱신: 이벤트 발행 실패 개수
			recordFailure(event);
			log.error("Kafka 집계 이벤트 발행 요청 실패: eventId={}, market={}, symbol={}",
				event.eventId(), event.market(), event.symbol(), exception);
		}
	}

	private void recordFailure(TradeExecutionEvent event) {
		// 모니터링 지표 갱신: 이벤트 발행 실패 수
		meterRegistry.counter("defomo.kafka.trade.events.publish.failures", "market", event.market()).increment();
	}
}
