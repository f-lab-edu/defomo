package com.defomo;

import com.defomo.marketdata.adapter.out.tossinvest.TossInvestProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Spring Boot 애플리케이션 진입점. 토스 API 설정 바인딩과 예약 작업을 활성화한다. */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(TossInvestProperties.class)
public class DefomoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DefomoApplication.class, args);
	}
}
