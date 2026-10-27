package com.defomo.alert.adapter.in.web;

import com.defomo.alert.adapter.out.persistence.AlertEntity;
import com.defomo.alert.domain.AlertPriceCondition;
import com.defomo.alert.domain.AlertStatus;
import io.swagger.v3.oas.annotations.media.Schema;
import java.math.BigDecimal;
import java.time.Instant;

/** 알림 등록 및 조회 결과.
 *
 * @param id 알림 식별자
 * @param userId 사용자 식별자
 * @param market 시장 구분
 * @param symbol 종목 코드
 * @param priceCondition 가격 조건
 * @param targetPrice 목표 가격
 * @param status 알림 상태
 * @param createdAt 등록 시각
 * @param firedAt 조건 충족 시각
 */
@Schema(description = "사용자 목표가격 알림 정보")
public record AlertResponse(
	Long id,
	String userId,
	String market,
	String symbol,
	AlertPriceCondition priceCondition,
	BigDecimal targetPrice,
	AlertStatus status,
	Instant createdAt,
	Instant firedAt) {

	/** 저장된 알림 정보를 API 응답으로 변환한다.
	 *
	 * @param alert DB에서 조회하거나 저장한 알림
	 * @return API 응답 객체
	 */
	public static AlertResponse from(AlertEntity alert) {
		return new AlertResponse(
			alert.getId(),
			alert.getUserId(),
			alert.getMarket(),
			alert.getSymbol(),
			alert.getPriceCondition(),
			alert.getTargetPrice(),
			alert.getStatus(),
			alert.getCreatedAt(),
			alert.getFiredAt());
	}
}
