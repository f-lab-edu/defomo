package com.defomo.alert.adapter.out.persistence;

import com.defomo.alert.domain.AlertPriceCondition;
import com.defomo.alert.domain.AlertStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** 가격 이벤트에 해당하는 활성 알림 조건을 조회하고 상태를 변경한다. */
public interface AlertRepository extends JpaRepository<AlertEntity, Long> {

	@Query("""
		select alert from AlertEntity alert
		where alert.market = :market
		  and alert.symbol = :symbol
		  and alert.status = :activeStatus
		  and ((alert.priceCondition = :atOrAbove and alert.targetPrice <= :maxPrice)
		    or (alert.priceCondition = :atOrBelow and alert.targetPrice >= :minPrice))
		""")
	List<AlertEntity> findMatchingActiveAlerts(
		@Param("market") String market,
		@Param("symbol") String symbol,
		@Param("activeStatus") AlertStatus activeStatus,
		@Param("atOrAbove") AlertPriceCondition atOrAbove,
		@Param("atOrBelow") AlertPriceCondition atOrBelow,
		@Param("minPrice") BigDecimal minPrice,
		@Param("maxPrice") BigDecimal maxPrice);

	@Modifying
	@Query("""
		update AlertEntity alert
		set alert.status = :firedStatus, alert.firedAt = :firedAt
		where alert.id = :alertId and alert.status = :activeStatus
		""")
	int markFiredIfActive(
		@Param("alertId") Long alertId,
		@Param("activeStatus") AlertStatus activeStatus,
		@Param("firedStatus") AlertStatus firedStatus,
		@Param("firedAt") Instant firedAt);
}
