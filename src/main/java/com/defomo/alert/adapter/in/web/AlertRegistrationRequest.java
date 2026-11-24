package com.defomo.alert.adapter.in.web;

import com.defomo.alert.domain.AlertPriceCondition;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;

/** 목표가격 알림 등록 요청.
 *
 * @param userId 사용자 식별자
 * @param market 시장 구분(KR 또는 US)
 * @param symbol 종목 코드
 * @param targetPrice 목표 가격
 * @param priceCondition 가격 조건
 */
@Schema(description = "등록할 목표가격 알림 조건")
public record AlertRegistrationRequest(
	@Schema(description = "사용자 식별자", example = "user-001")
	@NotBlank(message = "userId는 필수입니다.")
	@Size(max = 100, message = "userId는 100자 이하여야 합니다.")
	String userId,
	@Schema(description = "시장 구분", example = "US")
	@NotBlank(message = "market은 필수입니다.")
	@Pattern(regexp = "(?i)^\\s*(KR|US)\\s*$", message = "market은 KR 또는 US여야 합니다.")
	String market,
	@Schema(description = "종목 코드", example = "AAPL")
	@NotBlank(message = "symbol은 필수입니다.")
	@Size(max = 32, message = "symbol은 32자 이하여야 합니다.")
	String symbol,
	@Schema(description = "목표 가격", example = "200.00")
	@NotNull(message = "targetPrice는 필수입니다.")
	@DecimalMin(value = "0", inclusive = false, message = "targetPrice는 0보다 커야 합니다.")
	@Digits(integer = 16, fraction = 8, message = "targetPrice는 최대 16자리 정수와 소수점 8자리까지 입력할 수 있습니다.")
	BigDecimal targetPrice,
	@Schema(description = "가격 조건", example = "AT_OR_ABOVE")
	@NotNull(message = "priceCondition은 필수입니다.")
	AlertPriceCondition priceCondition) {
}
