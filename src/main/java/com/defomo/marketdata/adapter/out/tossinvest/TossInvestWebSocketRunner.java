package com.defomo.marketdata.adapter.out.tossinvest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/** 국내·미국 체결 웹소켓을 연결하고 구독 결과와 체결 데이터를 로그에 기록한다.
 * <p>자동 재연결과 체결 데이터 저장은 담당하지 않는다.</p>
 */
@Component
public class TossInvestWebSocketRunner implements ApplicationRunner, AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(TossInvestWebSocketRunner.class);
	/** 웹소켓 연결당 허용하는 최대 종목 수. */
	private static final int MAX_SYMBOLS_PER_MARKET = 100;
	/** 연결 유지를 위한 PING 전송 간격(초). */
	private static final long KEEPALIVE_SECONDS = 60;

	private final TossInvestProperties properties;
	private final TossInvestTokenClient tokenClient;
	private final ObjectMapper objectMapper;
	private final HttpClient httpClient;
	private final Set<WebSocket> sockets = ConcurrentHashMap.newKeySet();

	public TossInvestWebSocketRunner(
		TossInvestProperties properties,
		TossInvestTokenClient tokenClient,
		ObjectMapper objectMapper) {
		this.properties = properties;
		this.tokenClient = tokenClient;
		this.objectMapper = objectMapper;
		this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
	}

	/**
	 * 설정된 종목의 체결 구독을 앱 시작 시 연결한다.
	 *
	 * <p>인증 정보가 모두 비어 있으면 연결 생략. 일부만 설정하면 오류.
	 * 연결하려면 구독 종목이 하나 이상 필요.</p>
	 *
	 * @throws IllegalStateException 인증 정보 일부 누락 또는 구독 종목 미설정
	 */
	@Override
	public void run(ApplicationArguments args) {
		boolean hasClientId = StringUtils.hasText(properties.getClientId());
		boolean hasClientSecret = StringUtils.hasText(properties.getClientSecret());
		if (!hasClientId && !hasClientSecret) {
			log.info("TossInvest WebSocket integration is skipped because credentials are not configured");
			return;
		}
		if (!hasClientId || !hasClientSecret) {
			throw new IllegalStateException("Configure both TOSSINVEST_CLIENT_ID and TOSSINVEST_CLIENT_SECRET");
		}

		List<String> krSymbols = normalizedSymbols(properties.getMarketData().getKrSymbols());
		List<String> usSymbols = normalizedSymbols(properties.getMarketData().getUsSymbols());
		validateSubscriptionSize("KR", krSymbols);
		validateSubscriptionSize("US", usSymbols);
		if (krSymbols.isEmpty() && usSymbols.isEmpty()) {
			throw new IllegalStateException("Configure at least one TossInvest KR or US symbol");
		}

		String token = tokenClient.getAccessToken();
		connect("KR", "trade:kr", krSymbols, token);
		connect("US", "trade:us", usSymbols, token);
	}

	/** 시장별 구독 목록을 전송하고 웹소켓 연결을 연다. */
	private void connect(String market, String subscriptionType, List<String> symbols, String token) {
		if (symbols.isEmpty()) {
			return;
		}

		try {
			// 구독 배열은 전체 목록을 나타내므로 종목을 한 번에 담는다.
			String declaration = objectMapper.writeValueAsString(List.of(Map.of(
				"type", subscriptionType,
				"codes", symbols)));
			WebSocket socket = httpClient.newWebSocketBuilder()
				.connectTimeout(Duration.ofSeconds(10))
				.header("Authorization", "Bearer " + token)
				.buildAsync(URI.create(properties.getWebsocketUrl()), new TradeListener(market, declaration))
				.get(15, TimeUnit.SECONDS);
			sockets.add(socket);
			log.info("TossInvest {} trade WebSocket connected; {} symbols requested", market, symbols.size());
		} catch (Exception exception) {
			throw new IllegalStateException("Could not connect to TossInvest " + market + " trade WebSocket", exception);
		}
	}

	private static void validateSubscriptionSize(String market, List<String> symbols) {
		if (symbols.size() > MAX_SYMBOLS_PER_MARKET) {
			throw new IllegalArgumentException(
				market + " subscription has " + symbols.size() + " symbols; maximum is " + MAX_SYMBOLS_PER_MARKET);
		}
	}

	private static List<String> normalizedSymbols(List<String> symbols) {
		return symbols.stream()
			.filter(StringUtils::hasText)
			.map(String::trim)
			.distinct()
			.toList();
	}

	/** 열린 각 연결에 텍스트 PING을 전송한다. */
	@org.springframework.scheduling.annotation.Scheduled(fixedRate = KEEPALIVE_SECONDS * 1000)
	public void sendKeepalive() {
		for (WebSocket socket : sockets) {
			if (!socket.isOutputClosed()) {
				socket.sendText("PING", true).exceptionally(exception -> {
					log.warn("TossInvest WebSocket keepalive failed: {}", exception.getMessage());
					return null;
				});
			}
		}
	}

	/** 애플리케이션 종료 시 열린 웹소켓을 닫는다. */
	@Override
	public void close() {
		for (WebSocket socket : sockets) {
			socket.sendClose(WebSocket.NORMAL_CLOSURE, "application shutdown");
		}
		sockets.clear();
	}

	/** 웹소켓 프레임에서 구독 결과와 체결 데이터를 처리한다. */
	private final class TradeListener implements WebSocket.Listener {
		private final Logger listenerLog = LoggerFactory.getLogger(TradeListener.class);
		private final String market;
		private final String declaration;
		private final StringBuilder fragments = new StringBuilder();
		private final Set<String> firstSeenTopics = ConcurrentHashMap.newKeySet();

		/**
		 * @param market 로그에 사용할 시장 코드
		 * @param declaration 연결 후 전송할 JSON 구독 목록
		 */
		private TradeListener(String market, String declaration) {
			this.market = market;
			this.declaration = declaration;
		}

		@Override
		public void onOpen(WebSocket webSocket) {
			listenerLog.info("TossInvest {} WebSocket handshake succeeded", market);
			webSocket.request(1);
			webSocket.sendText(declaration, true).whenComplete((ignored, error) -> {
				if (error != null) {
					listenerLog.error("TossInvest {} subscription declaration failed", market, error);
				} else {
					listenerLog.info("TossInvest {} trade subscription declaration sent", market);
				}
			});
		}

		/** 분할된 텍스트 프레임을 모아 완성된 JSON 메시지를 처리한다. */
		@Override
		public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
				fragments.append(data);
			if (last) {
				handleFrame(fragments.toString());
				fragments.setLength(0);
			}
			// 다음 프레임을 요청해야 JDK 웹소켓 클라이언트가 메시지를 전달한다.
			webSocket.request(1);
			return null;
		}

		private void handleFrame(String frame) {
			try {
				JsonNode message = objectMapper.readTree(frame);
				String type = message.path("type").asText();
				switch (type) {
					case "subscriptions" -> handleSubscriptionAck(message);
					case "message" -> {
						listenerLog.info("TossInvest {} raw trade frame: {}", market, frame);
						handleTradeMessage(message);
					}
					case "error" -> listenerLog.error("TossInvest {} WebSocket error: {}", market,
						message.path("error").toString());
					case "pong" -> listenerLog.debug("TossInvest {} WebSocket pong received", market);
					default -> listenerLog.debug("TossInvest {} WebSocket frame: {}", market, frame);
				}
			} catch (Exception exception) {
				listenerLog.warn("Could not parse TossInvest {} WebSocket frame: {}", market, frame, exception);
			}
		}

		private void handleSubscriptionAck(JsonNode message) {
			JsonNode subscribed = message.path("subscribed");
			JsonNode rejected = message.path("rejected");
			listenerLog.info("TossInvest {} subscriptions acknowledged: accepted={}, rejected={}",
				market, subscribed.isArray() ? subscribed.size() : 0, rejected.isArray() ? rejected.size() : 0);
			if (rejected.isArray() && !rejected.isEmpty()) {
				listenerLog.error("TossInvest {} rejected subscriptions: {}", market, rejected);
			}
		}

		private void handleTradeMessage(JsonNode message) {
			String topic = message.path("topic").asText("unknown");
			JsonNode data = message.path("data");
			String symbol = topic.substring(topic.lastIndexOf(':') + 1);
			String timestamp = data.path("timestamp").asText("unknown");
			String price = data.path("price").asText("unknown");
			String volume = data.path("volume").asText("unknown");
			String currency = data.path("currency").asText("unknown");
			if (firstSeenTopics.add(topic)) {
				listenerLog.info(
					"First TossInvest {} trade received: symbol={}, timestamp={}, price={}, volume={}, currency={}",
					market, symbol, timestamp, price, volume, currency);
			} else {
				listenerLog.debug(
					"TossInvest {} trade received: symbol={}, timestamp={}, price={}, volume={}, currency={}",
					market, symbol, timestamp, price, volume, currency);
			}
		}

		@Override
		public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
			sockets.remove(webSocket);
			listenerLog.warn("TossInvest {} WebSocket closed: code={}, reason={}", market, statusCode, reason);
			return null;
		}

		@Override
		public void onError(WebSocket webSocket, Throwable error) {
			sockets.remove(webSocket);
			listenerLog.error("TossInvest {} WebSocket failed", market, error);
		}
	}
}
