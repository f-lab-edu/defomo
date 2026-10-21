package com.defomo.marketdata.adapter.out.tossinvest;

import java.time.Duration;
import java.time.Instant;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClient;

/** OAuth 액세스 토큰을 발급하고 만료 30초 전까지 메모리에 재사용한다. */
@Component
public class TossInvestTokenClient {

	private final TossInvestProperties properties;
	private final RestClient restClient;
	private CachedToken cachedToken;

	public TossInvestTokenClient(TossInvestProperties properties, RestClient.Builder restClientBuilder) {
		this.properties = properties;
		SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
		requestFactory.setConnectTimeout(Duration.ofSeconds(10));
		requestFactory.setReadTimeout(Duration.ofSeconds(15));
		this.restClient = restClientBuilder
			.baseUrl(properties.getRestBaseUrl())
			.requestFactory(requestFactory)
			.build();
	}

	/**
	 * 유효한 액세스 토큰 반환. 토큰이 없거나 만료까지 30초 이하이면 재발급.
	 *
	 * @return Bearer 접두어를 제외한 액세스 토큰
	 * @throws IllegalStateException 인증 정보 누락 또는 잘못된 토큰 응답
	 * @throws org.springframework.web.client.RestClientException 토큰 API 요청 실패
	 */
	public synchronized String getAccessToken() {
		// 웹소켓 연결 중 만료 방지를 위해 토큰 만료 30초 전에 갱신.
		if (cachedToken != null && Instant.now().isBefore(cachedToken.expiresAt().minusSeconds(30))) {
			return cachedToken.value();
		}

		if (isBlank(properties.getClientId()) || isBlank(properties.getClientSecret())) {
			throw new IllegalStateException(
				"Set TOSSINVEST_CLIENT_ID and TOSSINVEST_CLIENT_SECRET before connecting to TossInvest");
		}

		MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
		form.add("grant_type", "client_credentials");
		form.add("client_id", properties.getClientId());
		form.add("client_secret", properties.getClientSecret());

		TokenResponse response = restClient.post()
			.uri("/oauth2/token")
			.contentType(MediaType.APPLICATION_FORM_URLENCODED)
			.body(form)
			.retrieve()
			.body(TokenResponse.class);

		if (response == null || isBlank(response.accessToken()) || response.expiresIn() <= 0) {
			throw new IllegalStateException("TossInvest returned an invalid OAuth token response");
		}

		cachedToken = new CachedToken(response.accessToken(), Instant.now().plusSeconds(response.expiresIn()));
		return cachedToken.value();
	}

	private static boolean isBlank(String value) {
		return value == null || value.isBlank();
	}

	private record CachedToken(String value, Instant expiresAt) {
	}

	private record TokenResponse(
		@com.fasterxml.jackson.annotation.JsonProperty("access_token") String accessToken,
		@com.fasterxml.jackson.annotation.JsonProperty("expires_in") long expiresIn) {
	}
}
