package com.defomo.alert.application;

import com.defomo.alert.adapter.out.persistence.AlertEntity;
import com.defomo.alert.adapter.out.persistence.AlertRepository;
import com.defomo.alert.domain.AlertPriceCondition;
import java.math.BigDecimal;
import java.util.Locale;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/** 사용자 알림 조건을 검증하고 DB에 등록한다. */
@Service
public class AlertConditionRegistrationService {

	private final AlertRepository alertRepository;

	public AlertConditionRegistrationService(AlertRepository alertRepository) {
		this.alertRepository = alertRepository;
	}

	public AlertEntity register(
		String userId,
		String market,
		String symbol,
		BigDecimal targetPrice,
		AlertPriceCondition priceCondition) {
		if (!StringUtils.hasText(userId) || !StringUtils.hasText(market) || !StringUtils.hasText(symbol)) {
			throw new IllegalArgumentException("User, market, and symbol are required");
		}
		String normalizedMarket = market.trim().toUpperCase(Locale.ROOT);
		if (!normalizedMarket.equals("KR") && !normalizedMarket.equals("US")) {
			throw new IllegalArgumentException("Market must be KR or US");
		}
		if (targetPrice == null || targetPrice.signum() <= 0 || targetPrice.scale() > 8) {
			throw new IllegalArgumentException("Target price must be positive and have at most 8 decimal places");
		}
		if (priceCondition == null) {
			throw new IllegalArgumentException("Price condition is required");
		}

		AlertEntity alert = new AlertEntity(
			userId.trim(),
			normalizedMarket,
			symbol.trim().toUpperCase(Locale.ROOT),
			priceCondition,
			targetPrice);
		return alertRepository.save(alert);
	}
}
