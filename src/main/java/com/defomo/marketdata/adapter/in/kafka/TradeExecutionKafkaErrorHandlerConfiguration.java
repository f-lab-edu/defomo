package com.defomo.marketdata.adapter.in.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/** Kafka 체결 Consumer의 재시도 정책을 설정한다. */
@Configuration
public class TradeExecutionKafkaErrorHandlerConfiguration {

	/**
	 * 일시적 처리 오류는 최대 10분 동안 지수 간격으로 재시도하고,
	 * JSON 변환 오류는 재시도 없이 건너뛴다.
	 *
	 * @return Kafka 리스너의 공통 오류 처리기
	 */
	@Bean
	public DefaultErrorHandler kafkaErrorHandler() {
		ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
		backOff.setMaxInterval(30_000L);
		backOff.setMaxElapsedTime(600_000L);

		DefaultErrorHandler errorHandler = new DefaultErrorHandler(backOff);
		errorHandler.addNotRetryableExceptions(JsonProcessingException.class);
		return errorHandler;
	}
}
