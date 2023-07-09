/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.transaction;

import io.dbsink.connector.sink.event.ChangeEvent;
import io.dbsink.connector.sink.event.DataChangeEvent;
import io.dbsink.connector.sink.event.SchemaChangeEvent;
import io.dbsink.connector.sink.event.TransactionEvent;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Transaction
 *
 * @author Wang Wei
 * @time: 2023-06-10
 */
public class Transaction {
    private String id;

    private List<ChangeEvent> events;

    private boolean hasDataChangeEvent;

    public Transaction(String id) {
        this.id = id;
        this.events = new ArrayList<>();
    }

    public String getId() {
        return id;
    }

    public List<ChangeEvent> getEvents() {
        return Collections.unmodifiableList(events);
    }

    public void addEvent(ChangeEvent event) {
        if (event instanceof DataChangeEvent) {
            hasDataChangeEvent = true;
        }
        events.add(event);
    }

    public boolean hasDataChangeEvent() {
        return hasDataChangeEvent;
    }

    public static Transaction of(ChangeEvent event) {
        Transaction transaction = new Transaction(null);
        transaction.events = new ArrayList();
        transaction.addEvent(event);
        return transaction;
    }
}
