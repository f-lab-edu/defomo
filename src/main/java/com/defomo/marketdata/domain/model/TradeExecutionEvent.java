package com.defomo.marketdata.domain.model;

import java.math.BigDecimal;
import java.time.Instant;

/** 짧은 수집 구간의 체결 가격 범위와 해당 구간 정보를 담는다. */
public record TradeExecutionEvent(
	String eventId,
	String market,
	String symbol,
	int shard,
	BigDecimal minPrice,
	BigDecimal maxPrice,
	int tradeCount,
	Instant batchStartTime,
	Instant batchEndTime) {
}
