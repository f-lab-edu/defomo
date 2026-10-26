package com.defomo.marketdata.adapter.in.web;

import com.defomo.marketdata.application.AdminTradeInputService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.math.BigDecimal;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/** 관리자 체결 입력 API. */
@RestController
@RequestMapping("/api/admin/trades")
@Tag(name = "Admin", description = "관리자 체결 데이터 입력")
public class AdminTradeController {

	private final AdminTradeInputService adminTradeInputService;

	public AdminTradeController(AdminTradeInputService adminTradeInputService) {
		this.adminTradeInputService = adminTradeInputService;
	}

	/** 요청한 체결을 종목별 배치에 추가한다. */
	@PostMapping
	@ResponseStatus(HttpStatus.ACCEPTED)
	@Operation(summary = "체결 데이터 입력", description = "체결을 종목별로 집계하고, 배치가 완성되면 Kafka에 발행한다.")
	public void addTrade(@Valid @RequestBody AdminTradeRequest request) {
		adminTradeInputService.addTrade(request.market(), request.symbol(), request.price());
	}
}
