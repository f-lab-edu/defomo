package com.defomo.alert.application;

import com.defomo.alert.adapter.out.persistence.AlertEntity;
import com.defomo.alert.adapter.out.persistence.AlertRepository;
import com.defomo.alert.domain.AlertPriceCondition;
import com.defomo.alert.domain.AlertStatus;
import com.defomo.marketdata.domain.model.TradeExecutionEvent;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** 체결 가격 이벤트와 사용자의 목표가격 조건을 비교하고 결과를 저장한다. */
@Service
public class AlertConditionEvaluationService {

	private final AlertRepository alertRepository;

	public AlertConditionEvaluationService(AlertRepository alertRepository) {
		this.alertRepository = alertRepository;
	}

	//TODO - 체결 이벤트를 가져오고, 알림 조건 판별
	/**
	 * 체결 이벤트를 기준으로 조건을 충족한 알림을 찾고,
	 * 해당 알림을 한 번만 FIRED 상태로 변경한다.
	 */
	@Transactional
	public List<AlertNotificationCandidate> evaluate(TradeExecutionEvent event) {

		// 체결 이벤트의 종목과 가격 범위에 해당하면서
		// 아직 발송되지 않은 활성 알림을 조회한다.
		List<AlertEntity> matchingAlerts = alertRepository.findMatchingActiveAlerts(
				event.market(),
				event.symbol(),
				AlertStatus.ACTIVE,
				AlertPriceCondition.AT_OR_ABOVE,
				AlertPriceCondition.AT_OR_BELOW,
				event.minPrice(),
				event.maxPrice()
		);

		// 조건을 충족해 처음 FIRED 상태로 변경된 알림 정보를 모은다.
		List<AlertNotificationCandidate> candidates = new ArrayList<>();

		// 조회된 알림을 하나씩 처리한다.
		for (AlertEntity alert : matchingAlerts) {

			// 알림이 여전히 ACTIVE 상태인 경우에만
			// FIRED 상태로 변경한다.
			int changedRows = alertRepository.markFiredIfActive(
					alert.getId(),
					AlertStatus.ACTIVE,
					AlertStatus.FIRED,
					event.batchEndTime()
			);

			// 이미 다른 처리에서 FIRED로 변경된 알림이면
			// 중복 처리를 하지 않는다.
			if (changedRows == 0) {
				continue;
			}

			// 조건 충족 결과를 Consumer가 로그로 확인할 수 있도록 목록에 추가한다.
			candidates.add(
					new AlertNotificationCandidate(
							alert.getId(),
							alert.getMarket(),
							alert.getSymbol(),
							alert.getPriceCondition(),
							alert.getTargetPrice(),
							event.eventId()
					)
			);
		}

		// 이번 체결 이벤트에서 ACTIVE에서 FIRED로 처음 변경된 알림 목록을 반환한다.
		return candidates;
	}
}
