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

/** 집계된 체결 이벤트를 샤드에 지정된 Kafka 파티션으로 발행한다. */
@Component
public class KafkaTradeExecutionEventPublisher implements TradeExecutionEventPublisher {

	private static final Logger log = LoggerFactory.getLogger(KafkaTradeExecutionEventPublisher.class);

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ObjectMapper objectMapper;
	private final String topicName;
	private final int partitionCount;
	private final int shardCount;

	public KafkaTradeExecutionEventPublisher(
		KafkaTemplate<String, String> kafkaTemplate,
		ObjectMapper objectMapper,
		@Value("${defomo.kafka.trade-executions-topic}") String topicName,
		@Value("${defomo.kafka.trade-executions-partitions:6}") int partitionCount,
		@Value("${defomo.market-data.shard-count:6}") int shardCount) {
		if (partitionCount < 1 || shardCount < 1 || shardCount > partitionCount) {
			throw new IllegalArgumentException("Shard count must be positive and no greater than the partition count");
		}
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
		this.topicName = topicName;
		this.partitionCount = partitionCount;
		this.shardCount = shardCount;
	}

	@Override
	public void publish(TradeExecutionEvent event) {
		try {
			if (event.shard() < 0 || event.shard() >= shardCount) {
				throw new IllegalArgumentException("Event shard is outside the configured shard range");
			}
			String payload = objectMapper.writeValueAsString(event);
			int basePartition = Math.floorMod((event.market() + ":" + event.symbol()).hashCode(), partitionCount);
			int partition = (basePartition + event.shard()) % partitionCount;
			String key = event.market() + ":" + event.symbol() + ":" + event.shard();
			kafkaTemplate.send(topicName, partition, key, payload)
				.whenComplete((result, error) -> {
					if (error != null) {
						log.error("Kafka 집계 이벤트 발행 실패: market={}, symbol={}, shard={}",
							event.market(), event.symbol(), event.shard(), error);
						return;
					}
					log.debug("Kafka 집계 이벤트 발행 완료: topic={}, partition={}, offset={}, market={}, symbol={}, shard={}",
						result.getRecordMetadata().topic(),
						result.getRecordMetadata().partition(),
						result.getRecordMetadata().offset(),
						event.market(), event.symbol(), event.shard());
				});
		} catch (JsonProcessingException exception) {
			log.error("Kafka 집계 이벤트 JSON 변환 실패: market={}, symbol={}", event.market(), event.symbol(), exception);
		}
	}
}
