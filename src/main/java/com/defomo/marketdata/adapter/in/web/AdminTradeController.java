package com.defomo.marketdata.adapter.in.web;

import com.defomo.marketdata.application.TradeExecutionInputService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.math.BigDecimal;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/** 관리자 체결 입력 API. */
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/admin/trades")
@Tag(name = "Admin", description = "관리자 체결 데이터 입력")
public class AdminTradeController {

	private final TradeExecutionInputService tradeExecutionInputService;

	/** 관리자 체결 입력을 처리한다. */
	@PostMapping
	@ResponseStatus(HttpStatus.ACCEPTED)
	@Operation(summary = "체결 데이터 입력", description = "체결마다 개별 Kafka 이벤트를 비동기 발행한다.")
	public void addTrade(@Valid @RequestBody AdminTradeRequest request) {
		tradeExecutionInputService.addTrade(request.market(), request.symbol(), request.price());
	}
}
