package com.defomo.marketdata.adapter.out.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** 체결 이벤트 토픽 설정. */
@Configuration
public class KafkaTopicConfiguration {

	private static final long TRADE_EXECUTION_RETENTION_MILLIS = 7L * 24 * 60 * 60 * 1000;

	@Bean
	public NewTopic tradeExecutionsTopic(
		@Value("${defomo.kafka.trade-executions-topic}") String topicName,
		@Value("${defomo.kafka.trade-executions-partitions:6}") int partitionCount) {
		return TopicBuilder.name(topicName)
			.partitions(partitionCount)
			.config(TopicConfig.RETENTION_MS_CONFIG, Long.toString(TRADE_EXECUTION_RETENTION_MILLIS))
			.config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
			.build();
	}
}
