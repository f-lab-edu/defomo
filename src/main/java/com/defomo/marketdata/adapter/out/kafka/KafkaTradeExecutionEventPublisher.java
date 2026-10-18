package com.defomo.marketdata.adapter.out.kafka;

import com.defomo.marketdata.application.port.out.TradeExecutionEventPublisher;
import com.defomo.marketdata.domain.model.TradeExecutionEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** 집계된 체결 이벤트를 Kafka에 발행한다. */
@Component
public class KafkaTradeExecutionEventPublisher implements TradeExecutionEventPublisher {

	private static final Logger log = LoggerFactory.getLogger(KafkaTradeExecutionEventPublisher.class);

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ObjectMapper objectMapper;
	private final String topicName;

	public KafkaTradeExecutionEventPublisher(
		KafkaTemplate<String, String> kafkaTemplate,
		ObjectMapper objectMapper,
		@Value("${defomo.kafka.trade-executions-topic}") String topicName) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
		this.topicName = topicName;
	}

	@Override
	public void publish(TradeExecutionEvent event) {
		try {
			String payload = objectMapper.writeValueAsString(event);
			// 파티션과 키를 지정하지 않아 Kafka의 RoundRobinPartitioner가 파티션을 고르게 선택하게 한다.
			// 종목별 전체 순서보다 모든 파티션에 이벤트를 분산하는 것을 우선한다.
			kafkaTemplate.send(topicName, null, payload)
				.whenComplete((result, error) -> {
					if (error != null) {
						log.error("Kafka 집계 이벤트 발행 실패: market={}, symbol={}",
							event.market(), event.symbol(), error);
						return;
					}
					log.debug("Kafka 집계 이벤트 발행 완료: topic={}, partition={}, offset={}, market={}, symbol={}",
						result.getRecordMetadata().topic(),
						result.getRecordMetadata().partition(),
						result.getRecordMetadata().offset(),
						event.market(), event.symbol());
				});
		} catch (JsonProcessingException exception) {
			log.error("Kafka 집계 이벤트 JSON 변환 실패: market={}, symbol={}", event.market(), event.symbol(), exception);
		}
	}
}
