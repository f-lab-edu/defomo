package com.defomo.marketdata.adapter.out.tossinvest;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** {@code tossinvest.*} 설정에서 토스증권 OpenAPI 연결 정보와 시장별 구독 종목을 바인딩한다. */
@ConfigurationProperties(prefix = "tossinvest")
public class TossInvestProperties {

	/** OAuth 토큰 발급에 사용하는 클라이언트 ID. */
	private String clientId;
	/** OAuth 토큰 발급에 사용하는 비밀 키. 로그에 출력하지 않는다. */
	private String clientSecret;
	/** OAuth 토큰 발급용 REST API 기본 주소. */
	private String restBaseUrl = "https://openapi.tossinvest.com";
	/** 실시간 체결 수신용 웹소켓 주소. */
	private String websocketUrl = "wss://openapi-ws.tossinvest.com/ws/v1";
	/** 시장별 체결 구독 종목 코드 목록. */
	private final MarketData marketData = new MarketData();

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getClientSecret() {
		return clientSecret;
	}

	public void setClientSecret(String clientSecret) {
		this.clientSecret = clientSecret;
	}

	public String getRestBaseUrl() {
		return restBaseUrl;
	}

	public void setRestBaseUrl(String restBaseUrl) {
		this.restBaseUrl = restBaseUrl;
	}

	public String getWebsocketUrl() {
		return websocketUrl;
	}

	public void setWebsocketUrl(String websocketUrl) {
		this.websocketUrl = websocketUrl;
	}

	public MarketData getMarketData() {
		return marketData;
	}

	/** 국내·미국 시장별 체결 구독 종목. */
	public static class MarketData {
		/** 국내 주식 종목 코드 목록. 예: {@code 005930}. */
		private List<String> krSymbols = new ArrayList<>();
		/** 미국 주식 티커 목록. 예: {@code AAPL}. */
		private List<String> usSymbols = new ArrayList<>();

		public List<String> getKrSymbols() {
			return krSymbols;
		}

		public void setKrSymbols(List<String> krSymbols) {
			this.krSymbols = krSymbols == null ? new ArrayList<>() : krSymbols;
		}

		public List<String> getUsSymbols() {
			return usSymbols;
		}

		public void setUsSymbols(List<String> usSymbols) {
			this.usSymbols = usSymbols == null ? new ArrayList<>() : usSymbols;
		}
	}
}
