package com.defomo.alert.application;

import com.defomo.alert.adapter.out.persistence.AlertEntity;
import com.defomo.alert.adapter.out.persistence.AlertRepository;
import com.defomo.alert.domain.AlertPriceCondition;
import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** 사용자 알림 등록과 조회를 처리한다. */
@Service
@RequiredArgsConstructor
public class AlertService {

	private final AlertRepository alertRepository;

	/** 목표가격 알림을 등록한다.
	 *
	 * @param userId 사용자 식별자
	 * @param market 시장 구분
	 * @param symbol 종목 코드
	 * @param targetPrice 목표 가격
	 * @param priceCondition 가격 조건
	 * @return 저장된 알림
	 */
	@Transactional
	public AlertEntity register(
		String userId,
		String market,
		String symbol,
		BigDecimal targetPrice,
		AlertPriceCondition priceCondition) {
		AlertEntity alert = new AlertEntity(
			userId.trim(),
			market.trim().toUpperCase(Locale.ROOT),
			symbol.trim().toUpperCase(Locale.ROOT),
			priceCondition,
			targetPrice);
		return alertRepository.save(alert);
	}

	/** 전체 알림을 페이지 단위로 조회한다.
	 *
	 * @param pageable 페이지 및 정렬 조건
	 * @return 알림 페이지
	 */
	@Transactional(readOnly = true)
	public Page<AlertEntity> findAll(Pageable pageable) {
		return alertRepository.findAll(pageable);
	}

	/** 사용자 ID에 해당하는 알림 전체를 조회한다.
	 *
	 * @param userId 사용자 식별자
	 * @return 최신순으로 정렬된 사용자의 알림 목록
	 */
	@Transactional(readOnly = true)
	public List<AlertEntity> findByUserId(String userId) {
		return alertRepository.findByUserIdOrderByCreatedAtDescIdDesc(userId.trim());
	}
}
