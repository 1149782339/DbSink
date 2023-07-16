package io.dbsink.connector.sink.util;

import java.sql.SQLException;

/**
 * Time Tracker used to handle sql exception {@link SQLException}
 * switch to upsert mode from insert mode when duplicate key
 * sql exception happened
 *
 * @author: Wang Wei
 * @time: 2023-07-16
 */
public class TimeTracker {
    private long lastTimestamp;

    public TimeTracker() {
        lastTimestamp = System.currentTimeMillis();
    }

    public boolean isTimeElapsed(long interval) {
        long currentTimestamp = System.currentTimeMillis();
        long elapsedTime = currentTimestamp - lastTimestamp;
        if (elapsedTime >= interval) {
            lastTimestamp = currentTimestamp;
            return true;
        }
        return false;
    }
}
