/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.transaction;

/**
 * Sequence
 *
 * @author Wang Wei
 * @time: 2023-06-10
 */
public class SequenceNo {

    private volatile long sequenceNumber;

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public void waitSeqNoGreatEquals(long lastCommitted) throws InterruptedException {
        while (sequenceNumber < lastCommitted) {
            Thread.sleep(0);
        }
    }
}
