package com.defomo.alert.application;

import com.defomo.alert.domain.AlertPriceCondition;
import java.math.BigDecimal;

/** 체결 이벤트의 가격 조건을 충족한 알림 정보. */
public record AlertNotificationCandidate(
	Long alertId,
	String market,
	String symbol,
	AlertPriceCondition priceCondition,
	BigDecimal targetPrice,
	String tradeEventId) {
}
