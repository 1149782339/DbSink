/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.trigger;

import io.dbsink.connector.sink.util.TimestampUtil;

import java.time.Duration;

/**
 * Delay trigger used by {@link io.dbsink.connector.sink.applier.JdbcApplier}
 * to implement delayed triggering events
 *
 * @author: Wang Wei
 * @time: 2023-06-15
 */
public class DelayTrigger implements Trigger {

    private final long duration;

    private long timestamp;

    private final Trigger trigger;

    public DelayTrigger(Duration duration, Trigger trigger) {
        this.duration = duration.toMillis();
        this.trigger = trigger;
    }

    public void set() {
        timestamp = TimestampUtil.currentTimestamp();
        trigger.set();
    }


    public void reset() {
        long currentTimestamp = TimestampUtil.currentTimestamp();
        if (currentTimestamp - timestamp >= duration) {
            timestamp = currentTimestamp;
            trigger.reset();
        }
    }

}
