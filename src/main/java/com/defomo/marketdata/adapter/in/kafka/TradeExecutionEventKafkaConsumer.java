package com.defomo.marketdata.adapter.in.kafka;

import com.defomo.alert.application.AlertConditionEvaluationService;
import com.defomo.alert.application.AlertNotificationCandidate;
import com.defomo.marketdata.domain.model.TradeExecutionEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka 체결 이벤트를 읽어 사용자 알림 조건 판별을 요청한다.
 *
 * <p>메시지 역직렬화와 판별 결과 로그 기록을 담당하며,
 * 알림 조회와 상태 변경은 판별 서비스에 위임한다.</p>
 */
@Component
public class TradeExecutionEventKafkaConsumer {

	private static final Logger log = LoggerFactory.getLogger(TradeExecutionEventKafkaConsumer.class);
	private final ObjectMapper objectMapper;
	private final AlertConditionEvaluationService alertConditionEvaluationService;

	/**
	 * 체결 이벤트를 변환하고 알림 조건을 판별하는 데 필요한 객체를 주입받는다.
	 *
	 * @param objectMapper Kafka 메시지 JSON을 체결 이벤트 객체로 변환하는 매퍼
	 * @param alertConditionEvaluationService 체결 이벤트에 맞는 알림 조건을 판별하는 서비스
	 */
	public TradeExecutionEventKafkaConsumer(
		ObjectMapper objectMapper,
		AlertConditionEvaluationService alertConditionEvaluationService) {
		this.objectMapper = objectMapper;
		this.alertConditionEvaluationService = alertConditionEvaluationService;
	}

	/**
	 * Kafka 체결 이벤트 한 건을 읽어 조건에 맞는 알림의 처리 결과를 기록한다.
	 *
	 * <p>메시지를 체결 이벤트로 변환한 뒤 판별 서비스에 전달한다.
	 * 서비스는 이벤트의 시장·종목·가격 범위에 맞는 활성 알림을 조회하고,
	 * 조건을 만족한 알림을 FIRED 상태로 변경한다.
	 * 이 메서드는 새로 조건을 충족한 알림을 로그로 남기며 실제 사용자에게 알림을 보내지는 않는다.</p>
	 *
	 * @param record Kafka에서 전달된 레코드. 값에는 JSON 형식의 체결 이벤트가 들어 있다.
	 * @throws IOException 레코드 값을 체결 이벤트 JSON으로 변환하지 못한 경우
	 */
	@KafkaListener(topics = "${defomo.kafka.trade-executions-topic}")
	public void consume(ConsumerRecord<String, String> record) throws IOException {
		// Kafka 레코드의 값은 JSON 문자열이므로, 서비스에서 다룰 체결 이벤트 객체로 변환한다.
		// JSON 형식이 잘못되었거나 필드 변환에 실패하면 예외를 Kafka 리스너 컨테이너로 전달한다.
		TradeExecutionEvent event = objectMapper.readValue(record.value(), TradeExecutionEvent.class);

		// 시장·종목·가격 범위에 해당하는 활성 알림을 조회하고 조건 충족 처리를 요청한다.
		// 최초로 조건을 충족한 알림은 ACTIVE에서 FIRED로 변경된다.
		List<AlertNotificationCandidate> candidates = alertConditionEvaluationService.evaluate(event);

		// 조건을 만족한 알림이 없으면 반복 로그를 줄이기 위해 상세 수준으로 기록하고 종료한다.
		if (candidates.isEmpty()) {
			log.debug("목표가격 조건을 충족한 알림 없음: market={}, symbol={}", event.market(), event.symbol());
			return;
		}

		// 이번 체결 이벤트에서 처음 조건을 충족한 알림 정보를 로그로 기록한다.
		// 현재 단계에서는 알림 정보를 별도 이벤트 테이블에 저장하거나 실제 채널로 전송하지 않는다.
		for (AlertNotificationCandidate candidate : candidates) {
			log.info("목표가격 조건 충족: alertId={}, market={}, symbol={}, condition={}, targetPrice={}, tradeEventId={}",
				candidate.alertId(),
				candidate.market(),
				candidate.symbol(),
				candidate.priceCondition(),
				candidate.targetPrice(),
				candidate.tradeEventId());
		}
	}
}
