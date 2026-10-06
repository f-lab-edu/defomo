package com.defomo.marketdata.application.port.out;

import com.defomo.marketdata.domain.model.TradeExecutionEvent;

/** 체결 이벤트 발행 기능. */
public interface TradeExecutionEventPublisher {

	/**
	 * 집계된 체결 가격 범위 이벤트를 발행한다.
	 *
	 * @param event 발행할 체결 이벤트
	 */
	void publish(TradeExecutionEvent event);
}
