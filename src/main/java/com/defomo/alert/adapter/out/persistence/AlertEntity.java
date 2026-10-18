package com.defomo.alert.adapter.out.persistence;

import com.defomo.alert.domain.AlertPriceCondition;
import com.defomo.alert.domain.AlertStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;

/** DB에 저장된 사용자 목표가격 알림 조건. */
@Entity
@Table(name = "alerts", indexes = {
	@Index(name = "idx_alert_condition_lookup", columnList = "market, symbol, status, price_condition, target_price")
})
public class AlertEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "user_id", nullable = false, length = 100)
	private String userId;

	@Column(nullable = false, length = 16)
	private String market;

	@Column(nullable = false, length = 32)
	private String symbol;

	@Enumerated(EnumType.STRING)
	@Column(name = "price_condition", nullable = false, length = 24)
	private AlertPriceCondition priceCondition;

	@Column(name = "target_price", nullable = false, precision = 24, scale = 8)
	private BigDecimal targetPrice;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false, length = 16)
	private AlertStatus status;

	@Column(name = "created_at", nullable = false)
	private Instant createdAt;

	@Column(name = "fired_at")
	private Instant firedAt;

	protected AlertEntity() {
	}

	public AlertEntity(
		String userId,
		String market,
		String symbol,
		AlertPriceCondition priceCondition,
		BigDecimal targetPrice) {
		this.userId = userId;
		this.market = market;
		this.symbol = symbol;
		this.priceCondition = priceCondition;
		this.targetPrice = targetPrice;
		this.status = AlertStatus.ACTIVE;
	}

	@PrePersist
	void setCreatedAt() {
		if (createdAt == null) {
			createdAt = Instant.now();
		}
	}

	public Long getId() {
		return id;
	}

	public String getUserId() {
		return userId;
	}

	public String getMarket() {
		return market;
	}

	public String getSymbol() {
		return symbol;
	}

	public AlertPriceCondition getPriceCondition() {
		return priceCondition;
	}

	public BigDecimal getTargetPrice() {
		return targetPrice;
	}
}
