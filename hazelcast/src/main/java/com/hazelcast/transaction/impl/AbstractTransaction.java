/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.transaction.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public abstract class AbstractTransaction implements InternalTransaction {

    protected final List<TransactionRecord> records = new LinkedList<TransactionRecord>();
    protected final Map<Object, TransactionRecord> recordMap = new HashMap<Object, TransactionRecord>();
    protected final NodeEngine nodeEngine;
    protected final ILogger logger;
    protected final String txnId;
    protected long timeoutMillis;
    protected long startTimeMillis;

    public AbstractTransaction(NodeEngine nodeEngine, String txnId, long timeoutMillis) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.txnId = txnId;
        this.timeoutMillis = timeoutMillis;
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    @Override
    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    @Override
    public String getTxnId() {
        return txnId;
    }

    @Override
    public final TransactionRecord get(Object key) {
        return recordMap.get(key);
    }

    public final List<TransactionRecord> getRecords() {
        return records;
    }

    protected void addInternal(TransactionRecord record) {
        // there should be just one tx log for the same key. so if there is older we are removing it
        if (record instanceof KeyAwareTransactionRecord) {
            KeyAwareTransactionRecord keyAwareTransactionRecord = (KeyAwareTransactionRecord) record;
            TransactionRecord removed = recordMap.remove(keyAwareTransactionRecord.getKey());
            if (removed != null) {
                records.remove(removed);
            }
        }

        records.add(record);
        if (record instanceof KeyAwareTransactionRecord) {
            KeyAwareTransactionRecord keyAwareTransactionRecord = (KeyAwareTransactionRecord) record;
            recordMap.put(keyAwareTransactionRecord.getKey(), keyAwareTransactionRecord);
        }
    }

    @Override
    public final void remove(Object key) {
        TransactionRecord removed = recordMap.remove(key);
        if (removed != null) {
            records.remove(removed);
        }
    }
}
