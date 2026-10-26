package com.defomo.marketdata.adapter.in.web;

import io.swagger.v3.oas.annotations.media.Schema;
import java.math.BigDecimal;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/** 관리자 체결 입력 요청. */
@Schema(description = "입력할 체결 정보")
public record AdminTradeRequest(
	@Schema(description = "시장 구분", example = "US")
	@NotBlank(message = "market은 필수입니다.")
	@Pattern(regexp = "(?i)^\\s*(KR|US)\\s*$", message = "market은 KR 또는 US여야 합니다.")
	String market,
	@Schema(description = "종목 코드", example = "TEST01")
	@NotBlank(message = "symbol은 필수입니다.")
	@Size(max = 32, message = "symbol은 32자 이하여야 합니다.")
	String symbol,
	@Schema(description = "체결 가격", example = "101.20")
	@NotNull(message = "price는 필수입니다.")
	@DecimalMin(value = "0", inclusive = false, message = "price는 0보다 커야 합니다.")
	@Digits(integer = 100, fraction = 8, message = "price는 소수점 8자리 이하여야 합니다.")
	BigDecimal price) {
}
