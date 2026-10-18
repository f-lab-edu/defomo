package com.defomo.marketdata.domain.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * 한 종목의 체결 배치를 가격 범위로 요약한 이벤트.
 *
 * @param minPrice 배치에 포함된 체결 가격 중 최저가
 * @param maxPrice 배치에 포함된 체결 가격 중 최고가
 * @param batchStartTime 앱에서 배치를 생성한 시각
 * @param batchEndTime 앱에서 마지막 체결 가격을 배치에 추가한 시각
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
