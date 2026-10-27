package com.defomo.marketdata.application;

import java.math.BigDecimal;
import java.util.Locale;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/** 관리자 API로 받은 체결을 집계기에 전달한다. */
@Service
@RequiredArgsConstructor
public class AdminTradeInputService {

	private final TradeExecutionBatchAggregator batchAggregator;

	public void addTrade(String market, String symbol, BigDecimal price) {
		String normalizedMarket = market.trim().toUpperCase(Locale.ROOT);
		String normalizedSymbol = symbol.trim().toUpperCase(Locale.ROOT);
		batchAggregator.accept(normalizedMarket, normalizedSymbol, price);
	}
}
