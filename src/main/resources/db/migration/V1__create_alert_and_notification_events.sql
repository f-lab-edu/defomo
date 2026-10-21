CREATE TABLE alerts (
    id BIGINT NOT NULL AUTO_INCREMENT,
    user_id VARCHAR(100) NOT NULL,
    market VARCHAR(16) NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    price_condition VARCHAR(24) NOT NULL,
    target_price DECIMAL(24, 8) NOT NULL,
    status VARCHAR(16) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL,
    fired_at TIMESTAMP(6) NULL,
    PRIMARY KEY (id),
    INDEX idx_alert_condition_lookup (market, symbol, status, price_condition, target_price)
);

CREATE TABLE notification_events (
    event_id CHAR(36) NOT NULL,
    alert_id BIGINT NOT NULL,
    trade_event_id CHAR(36) NOT NULL,
    min_price DECIMAL(24, 8) NOT NULL,
    max_price DECIMAL(24, 8) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (event_id),
    CONSTRAINT uq_notification_events_alert UNIQUE (alert_id),
    CONSTRAINT fk_notification_events_alert FOREIGN KEY (alert_id) REFERENCES alerts (id),
    INDEX idx_notification_events_created_at (created_at)
);
