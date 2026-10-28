package com.defomo.alert.adapter.in.web;

import com.defomo.alert.adapter.out.persistence.AlertEntity;
import com.defomo.alert.application.AlertService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Size;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/** 관리자 알림 등록과 조회 API. */
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/admin/alerts")
@Tag(name = "Admin Alerts", description = "관리자 사용자 목표가격 알림 관리")
public class AdminAlertController {

	private static final int DEFAULT_PAGE_SIZE = 20;
	private static final int MAX_PAGE_SIZE = 100;

	private final AlertService alertService;

	/** 요청한 목표가격 조건을 등록한다.
	 *
	 * @param request 등록할 사용자, 종목, 가격 조건
	 * @return 등록된 알림 정보
	 */
	@PostMapping
	@ResponseStatus(HttpStatus.CREATED)
	@Operation(summary = "목표가격 알림 등록")
	public AlertResponse register(@Valid @RequestBody AlertRegistrationRequest request) {
		AlertEntity alert = alertService.register(
			request.userId(), request.market(), request.symbol(), request.targetPrice(), request.priceCondition());
		return AlertResponse.from(alert);
	}

	/** 전체 알림을 페이지 단위로 조회한다.
	 *
	 * @param page 조회할 페이지 번호
	 * @param size 페이지 크기
	 * @return 알림 페이지
	 */
	@GetMapping
	@Operation(summary = "전체 알림 조회", description = "생성 시각이 최신인 알림부터 페이지 단위로 조회한다.")
	public Page<AlertResponse> findAll(
		@RequestParam(defaultValue = "0") @Min(0) int page,
		@RequestParam(defaultValue = "20") @Min(1) @Max(MAX_PAGE_SIZE) int size) {
		Pageable pageable = PageRequest.of(page, size, Sort.by(
			Sort.Order.desc("createdAt"), Sort.Order.desc("id")));
		return alertService.findAll(pageable).map(AlertResponse::from);
	}

	/** 지정한 사용자의 알림 전체를 조회한다.
	 *
	 * @param userId 사용자 식별자
	 * @return 사용자의 전체 알림 목록
	 */
	@GetMapping("/users/{userId}")
	@Operation(summary = "사용자별 알림 조회", description = "해당 사용자의 알림을 생성 시각이 최신인 순서로 조회한다.")
	public List<AlertResponse> findByUserId(@PathVariable @NotBlank @Size(max = 100) String userId) {
		return alertService.findByUserId(userId).stream().map(AlertResponse::from).toList();
	}
}
