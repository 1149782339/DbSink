/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.event;

import java.util.Collections;
import java.util.List;

/**
 * Applier Event
 *
 * @author: Wang Wei
 * @time: 2023-06-15
 */
public class ApplierEvent {
    /**
     * Last committed transaction sequence number
     *
     */
    private long lastCommitted;
    /**
     * transaction sequence number
     *
     */
    private long sequenceNumber;

    private boolean canUseWriteSet;

    private List<ChangeEvent> events;

    private String transactionId;

    private boolean hasDataChangeEvent;

    public long getLastCommitted() {
        return lastCommitted;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public List<ChangeEvent> getEvents() {
        return Collections.unmodifiableList(events);
    }

    public void setLastCommitted(long lastCommitted) {
        this.lastCommitted = lastCommitted;
    }

    public String getTopic() {
        return events.get(events.size() - 1).getTopic();
    }

    public long getOffset() {
        return events.get(events.size() - 1).getOffset();
    }

    public Integer getPartition() {
        return events.get(events.size() - 1).getPartition();
    }

    public String getTransactionId() {
        return transactionId;
    }

    public boolean canUseWriteSet() {
        return canUseWriteSet;
    }

    public boolean hasDataChangeEvent() {
        return hasDataChangeEvent;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<ChangeEvent> events;

        private long lastCommitted;

        private long sequenceNumber;

        private String transactionId;

        private boolean canUseWriteSet;

        private boolean hasDataChangeEvent;

        public Builder records(List<ChangeEvent> events) {
            this.events = events;
            return this;
        }

        public Builder lastCommitted(long lastCommitted) {
            this.lastCommitted = lastCommitted;
            return this;
        }

        public Builder sequenceNumber(long sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
            return this;
        }

        public Builder transactionId(String transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public Builder canUseWriteSet(boolean canUseWriteSet) {
            this.canUseWriteSet = canUseWriteSet;
            return this;
        }

        public Builder hasDataChangeEvent(boolean hasDataChangeEvent) {
            this.hasDataChangeEvent = hasDataChangeEvent;
            return this;
        }

        public ApplierEvent build() {
            ApplierEvent applierEvent = new ApplierEvent();
            applierEvent.events = events;
            applierEvent.sequenceNumber= sequenceNumber;
            applierEvent.lastCommitted = lastCommitted;
            applierEvent.transactionId = transactionId;
            applierEvent.canUseWriteSet = canUseWriteSet;
            applierEvent.hasDataChangeEvent = hasDataChangeEvent;
            return applierEvent;
        }
    }

}
