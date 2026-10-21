ALTER TABLE notification_events
    MODIFY event_id VARCHAR(36) NOT NULL,
    MODIFY trade_event_id VARCHAR(36) NOT NULL;
