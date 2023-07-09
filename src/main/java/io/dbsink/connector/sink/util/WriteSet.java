/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.util;

import io.dbsink.connector.sink.annotation.ThreadSafe;
import io.dbsink.connector.sink.event.ApplierEvent;
import io.dbsink.connector.sink.event.ChangeEvent;
import io.dbsink.connector.sink.event.DataChangeEvent;
import io.dbsink.connector.sink.event.Operation;
import io.dbsink.connector.sink.relation.FieldsMetaData;
import io.dbsink.connector.sink.relation.TableId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Write set is used by parallel transactional applier.
 * We use it to adjust the last committed sequence number
 * for those transactions which can be applied parallel.
 *
 * @author Wang Wei
 * @time: 2023-06-09
 */
@ThreadSafe
public class WriteSet {
    private static long MAX_HISTORY_WRITE_SET_SIZE = 1000;

    private static final String HASH_DELIMITER = "\0%\0";

    private Map<Long, Long> historyWriteSet = new HashMap<>();

    private final HashUtil hashUtil;

    private long historyWriteSetStart;

    public WriteSet() {
        hashUtil = new HashUtil();
    }

    /**
     * Add one applierEvent into writeSet and adjust its last committed sequence number if possible
     *
     * @param event {@link ApplierEvent}
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    public synchronized void add(ApplierEvent event) {
        boolean exceedCapacity = false;
        long sequenceNumber = event.getSequenceNumber();
        long lastCommitted = event.getLastCommitted();
        long lastParent = historyWriteSetStart;
        boolean canUseWriteSet = event.canUseWriteSet();
        if (canUseWriteSet) {
            List<Long> writeSet = getWriteSet(event);
            if (writeSet.size() + historyWriteSet.size() > MAX_HISTORY_WRITE_SET_SIZE) {
                exceedCapacity = true;
            }
            for (Long key : writeSet) {
                Long lastVersion = historyWriteSet.get(key);
                if (lastVersion == null) {
                    if (!exceedCapacity) {
                        historyWriteSet.put(key, sequenceNumber);
                    }
                } else {
                    if (lastVersion > lastParent && lastVersion < sequenceNumber) {
                        lastParent = lastVersion;
                    }
                    historyWriteSet.put(key, sequenceNumber);
                }
            }
            lastCommitted = Math.min(lastParent, lastCommitted);
            event.setLastCommitted(lastCommitted);
        }
        if (exceedCapacity || !canUseWriteSet) {
            historyWriteSetStart = sequenceNumber;
            historyWriteSet.clear();
        }
    }

    private List<Long> getWriteSet(ApplierEvent applierEvent) {
        List<Long> writeSet = new ArrayList<>(applierEvent.getEvents().size());
        for (ChangeEvent event : applierEvent.getEvents()) {
            if (!(event instanceof DataChangeEvent)) {
                continue;
            }
            final DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            final TableId tableId = dataChangeEvent.getTableId();
            final FieldsMetaData fieldsMetaData = dataChangeEvent.getFieldsMetaData();
            final List<String> primaryKeyFields = fieldsMetaData.getPrimaryKeyFieldNames();
            final List<String> row = new ArrayList<>(fieldsMetaData.getFieldSchemas().size() + 5);
            if (tableId.getCatalog() != null) {
                row.add(tableId.getCatalog());
            }
            if (tableId.getSchema() != null) {
                row.add(tableId.getSchema());
            }
            if (tableId.getTable() != null) {
                row.add(tableId.getTable());
            }
            Map<String, Object> values = dataChangeEvent.getOperation() == Operation.DELETE ?
                dataChangeEvent.getBeforeValues() : dataChangeEvent.getAfterValues();
            for (String fieldName : primaryKeyFields) {
                Object object = values.get(fieldName);
                row.add(object.toString());
            }
            writeSet.add(hashUtil.getHash(String.join(HASH_DELIMITER, row)));
        }
        return writeSet;
    }
}
