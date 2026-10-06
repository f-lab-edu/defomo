package com.defomo.marketdata.adapter.in.kafka;

import com.defomo.marketdata.domain.model.TradeExecutionEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka 체결 이벤트를 읽어 주요 항목을 기록한다. */
@Component
public class TradeExecutionEventKafkaConsumer {

	private static final Logger log = LoggerFactory.getLogger(TradeExecutionEventKafkaConsumer.class);
	private final ObjectMapper objectMapper;
	private final Set<String> firstSeenSymbols = ConcurrentHashMap.newKeySet();

	public TradeExecutionEventKafkaConsumer(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@KafkaListener(topics = "${defomo.kafka.trade-executions-topic}")
	public void consume(ConsumerRecord<String, String> record) throws IOException {
		TradeExecutionEvent event = objectMapper.readValue(record.value(), TradeExecutionEvent.class);
		String expectedKey = event.market() + ":" + event.symbol() + ":" + event.shard();
		if (!Objects.equals(record.key(), expectedKey)) {
			throw new IllegalArgumentException("Kafka message key does not match the event market, symbol, and shard");
		}

		String symbolKey = event.market() + ":" + event.symbol();
		if (firstSeenSymbols.add(symbolKey)) {
			log.info("Kafka 체결 집계 수신: market={}, symbol={}, shard={}, minPrice={}, maxPrice={}, count={}, start={}, end={}",
				event.market(), event.symbol(), event.shard(), event.minPrice(), event.maxPrice(), event.tradeCount(),
				event.batchStartTime(), event.batchEndTime());
			return;
		}
		log.debug("Kafka 체결 집계 수신: market={}, symbol={}, shard={}, minPrice={}, maxPrice={}, count={}, start={}, end={}",
			event.market(), event.symbol(), event.shard(), event.minPrice(), event.maxPrice(), event.tradeCount(),
			event.batchStartTime(), event.batchEndTime());
	}
}
