package com.defomo.marketdata.domain.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * 개별 체결 이벤트. 기존 소비자와의 호환성을 위해 가격 범위와 배치 시각 필드를 유지한다.
 *
 * @param minPrice 체결 가격 (maxPrice와 동일)
 * @param maxPrice 체결 가격 (minPrice와 동일)
 * @param batchStartTime 체결 수신 시각
 * @param batchEndTime 체결 수신 시각 (batchStartTime과 동일)
 */
public record TradeExecutionEvent(
	String eventId,
	String market,
	String symbol,
	BigDecimal minPrice,
	BigDecimal maxPrice,
	int tradeCount,
	Instant batchStartTime,
	Instant batchEndTime) {
}
