/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.core.Instance;
import com.hazelcast.core.Instance.InstanceType;
import com.hazelcast.core.Prefix;
import com.hazelcast.core.Transaction;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Data;
import com.hazelcast.util.Clock;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static com.hazelcast.nio.IOUtil.toObject;

public class TransactionImpl implements Transaction {

    public static final long DEFAULT_TXN_TIMEOUT = 30 * 1000;

    private final long id;
    private final FactoryImpl factory;
    private final List<TransactionRecord> transactionRecords = new CopyOnWriteArrayList<TransactionRecord>();

    private int status = TXN_STATUS_NO_TXN;
    private final ILogger logger;

    public TransactionImpl(FactoryImpl factory, long txnId) {
        this.id = txnId;
        this.factory = factory;
        this.logger = factory.getLoggingService().getLogger(this.getClass().getName());
    }

    public Data attachPutOp(String name, Object key, Data value, boolean newRecord) {
        return attachPutOp(name, key, value, 0, -1, newRecord, -1);
    }

    public void attachPutMultiOp(String name, Object key, Data value) {
        transactionRecords.add(new TransactionRecord(name, key, value, true));
    }

    public Data attachPutOp(String name, Object key, Data value, int timeout, long ttl, boolean newRecord) {
        return attachPutOp(name, key, value, timeout, ttl, newRecord, -1);
    }

    public Data attachPutOp(String name, Object key, Data value, long timeout, boolean newRecord, int index) {
        return attachPutOp(name, key, value, timeout, -1, newRecord, index);
    }

    public Data attachPutOp(String name, Object key, Data value, long timeout, long ttl, boolean newRecord, int index) {
        Instance.InstanceType instanceType = ConcurrentMapManager.getInstanceType(name);
        Object matchValue = (instanceType.isMultiMap()) ? toObject(value) : null;
        TransactionRecord rec = findTransactionRecord(name, key, matchValue);
        if (rec == null) {
            rec = new TransactionRecord(name, key, value, newRecord);
            rec.timeout = timeout;
            rec.ttl = ttl;
            rec.index = index;
            transactionRecords.add(rec);
            return null;
        } else {
            Data old = rec.value;
            rec.value = value;
            rec.removed = false;
            rec.index = index;
            return old;
        }
    }

    public void attachGetOp(String name, Object key, Data value) {
        Instance.InstanceType instanceType = ConcurrentMapManager.getInstanceType(name);
        TransactionRecord rec = findTransactionRecord(name, key);
        if (rec == null) {
            rec = new TransactionRecord(name, key, value);
            transactionRecords.add(rec);
        } else {
            rec.value = value;
        }
    }


    public Data attachRemoveOp(String name, Object key, Data value, boolean newRecord) {
        return attachRemoveOp(name, key, value, newRecord, 1);
    }

    public Data attachRemoveOp(String name, Object key, Data value, boolean newRecord, int valueCount) {
        Instance.InstanceType instanceType = ConcurrentMapManager.getInstanceType(name);
        Object matchValue = (instanceType.isMultiMap()) ? toObject(value) : null;
        TransactionRecord rec = findTransactionRecord(name, key, matchValue);
        Data oldValue = null;
        if (rec == null) {
            rec = new TransactionRecord(name, key, value, newRecord);
            transactionRecords.add(rec);
        } else {
            oldValue = rec.value;
            rec.value = value;
        }
        rec.valueCount = valueCount;
        rec.removed = true;
        return oldValue;
    }

    public void begin() throws IllegalStateException {
        if (status == TXN_STATUS_ACTIVE) {
            throw new IllegalStateException("Transaction is already active");
        }
        status = TXN_STATUS_ACTIVE;
    }

    public void commit() throws IllegalStateException {
        if (status != TXN_STATUS_ACTIVE) {
            throw new IllegalStateException("Transaction is not active");
        }
        status = TXN_STATUS_COMMITTING;
        try {
            ThreadContext.get().setCurrentFactory(factory);
            for (TransactionRecord record : transactionRecords) {
                record.commit();
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            finalizeTxn();
            status = TXN_STATUS_COMMITTED;
        }
    }

    public void rollback() throws IllegalStateException {
        if (status == TXN_STATUS_NO_TXN || status == TXN_STATUS_UNKNOWN
                || status == TXN_STATUS_COMMITTED || status == TXN_STATUS_ROLLED_BACK) {
            throw new IllegalStateException("Transaction is not ready to rollback. Status= "
                    + status);
        }
        status = TXN_STATUS_ROLLING_BACK;
        try {
            ThreadContext.get().setCurrentFactory(factory);
            final int size = transactionRecords.size();
            ListIterator<TransactionRecord> iter = transactionRecords.listIterator(size);
            while (iter.hasPrevious()) {
                TransactionRecord record = iter.previous();
                if (record.instanceType.isQueue()) {
                    rollbackMapTransactionRecordOfQueue(iter, record);
                }
                record.rollback();
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        } finally {
            finalizeTxn();
            status = TXN_STATUS_ROLLED_BACK;
        }
    }

    // Queues have two transaction records per operation;
    // one for queue key, one for map item. During rollback
    // we should rollback map record first.
    // See github issue#99 and TransactionTest.issue99TestQueueTakeAndDuringRollback
    private void rollbackMapTransactionRecordOfQueue(final ListIterator<TransactionRecord> iter,
                                                     final TransactionRecord queueTxRecord) {
        if (iter.hasPrevious()) {
            TransactionRecord prevRecord = iter.previous();
            if (prevRecord.instanceType != InstanceType.MAP) {
                logger.log(Level.WARNING, "Map#TransactionRecord is expected before a " +
                        "Queue#TransactionRecord, but got " + prevRecord.instanceType);
            } else if (!prevRecord.name.equals(Prefix.MAP + queueTxRecord.name)) {
                logger.log(Level.WARNING, "Expecting a record of " + Prefix.MAP + queueTxRecord.name
                        + " but got " + prevRecord.name);
            }
            // Rollback previous transaction record, even if it is not expected record.
            prevRecord.rollback();
        }
    }

    public boolean containsValue(String name, Object value) {
        for (TransactionRecord transactionRecord : transactionRecords) {
            if (transactionRecord.name.equals(name)) {
                if (!transactionRecord.removed) {
                    if (value.equals(toObject(transactionRecord.value))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public boolean containsEntry(String name, Object key, Object value) {
        TransactionRecord transactionRecord = findTransactionRecord(name, key, value);
        return transactionRecord != null && !transactionRecord.removed;
    }

    private TransactionRecord findTransactionRecord(String name, Object key) {
        for (TransactionRecord transactionRecord : transactionRecords) {
            if (transactionRecord.name.equals(name)) {
                if (transactionRecord.key != null) {
                    if (transactionRecord.key.equals(key)) {
                        return transactionRecord;
                    }
                }
            }
        }
        return null;
    }

    private TransactionRecord findTransactionRecord(String name, Object key, Object value) {
        for (TransactionRecord transactionRecord : transactionRecords) {
            if (transactionRecord.name.equals(name)) {
                if (transactionRecord.key != null) {
                    if (transactionRecord.key.equals(key)) {
                        final Object txValue = toObject(transactionRecord.value);
                        if (transactionRecord.instanceType.isMultiMap()) {
                            if (value == null && txValue == null) {
                                return transactionRecord;
                            } else if (value != null && value.equals(txValue)) {
                                return transactionRecord;
                            }
                        } else {
                            if (value == null) {
                                return transactionRecord;
                            } else if (value.equals(txValue)) {
                                return transactionRecord;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    public Data get(String name, Object key) {
        TransactionRecord rec = findTransactionRecord(name, key);
        if (rec == null) {
            return null;
        }
        if (rec.removed) {
            return null;
        }
        rec.lastAccess = Clock.currentTimeMillis();
        return rec.value;
    }

    public long getId() {
        return id;
    }

    public int getStatus() {
        return status;
    }

    public boolean has(String name, Object key) {
        return findTransactionRecord(name, key) != null;
    }

    public boolean has(String name, Object key, Object value) {
        return findTransactionRecord(name, key, value) != null;
    }

    public boolean isNew(String name, Object key) {
        TransactionRecord rec = findTransactionRecord(name, key);
        return (rec != null && !rec.removed && rec.newRecord);
    }

    public boolean isRemoved(String name, Object key) {
        TransactionRecord rec = findTransactionRecord(name, key);
        return (rec != null && rec.removed);
    }

    public int size(String name) {
        int size = 0;
        for (TransactionRecord transactionRecord : transactionRecords) {
            if (transactionRecord.name.equals(name)) {
                if (transactionRecord.removed) {
                    if (transactionRecord.instanceType.isSet()) {
                        size--;
                    } else if (!transactionRecord.newRecord) {
                        if (!transactionRecord.instanceType.isQueue()) {
                            size -= transactionRecord.valueCount;
                        }
                    }
                } else if (transactionRecord.newRecord) {
                    if (transactionRecord.instanceType.isList()) {
                        size += (Integer) toObject(transactionRecord.value);
                    } else {
                        size++;
                    }
                }
            }
        }
        return size;
    }

    public List<Map.Entry> newEntries(String name) {
        List<Map.Entry> lsEntries = null;
        for (TransactionRecord transactionRecord : transactionRecords) {
            if (transactionRecord.name.equals(name)) {
                if (!transactionRecord.removed) {
                    if (transactionRecord.value != null) {
                        if (transactionRecord.newRecord) {
                            if (lsEntries == null) {
                                lsEntries = new ArrayList<Map.Entry>(2);
                            }
                            lsEntries.add(BaseManager.createSimpleMapEntry(factory, name, transactionRecord.key, transactionRecord.value));
                        }
                    }
                }
            }
        }
        return lsEntries;
    }

    public void getMulti(String name, Object key, Collection col) {
        for (TransactionRecord transactionRecord : transactionRecords) {
            if (transactionRecord.name.equals(name)) {
                if (key.equals(transactionRecord.key)) {
                    if (!transactionRecord.removed && transactionRecord.newRecord) {
                        col.add(toObject(transactionRecord.value));
                    } else if (transactionRecord.removed) {
                        if (transactionRecord.value == null) {
                            col.clear();
                            return;
                        } else {
                            col.remove(toObject(transactionRecord.value));
                        }
                    }
                }
            }
        }
    }

    public Map newKeys(String name) {
        Map newEntries = null;
        for (TransactionRecord transactionRecord : transactionRecords) {
            if (transactionRecord.name.equals(name)) {
                if (!transactionRecord.removed) {
                    if (transactionRecord.value != null) {
                        if (transactionRecord.newRecord) {
                            if (newEntries == null) {
                                newEntries = new HashMap();
                            }
                            newEntries.put(transactionRecord.key, transactionRecord.value);
                        }
                    }
                }
            }
        }
        return newEntries;
    }

    @Override
    public String toString() {
        return "TransactionImpl [" + id + "] status: " + status;
    }

    private void finalizeTxn() {
        transactionRecords.clear();
        status = TXN_STATUS_NO_TXN;
        ThreadContext.get().finalizeTxn();
    }

    private class TransactionRecord {
        public String name;

        public Object key;

        public Data value;

        public boolean removed = false;

        public boolean newRecord = false;

        public boolean getRecord = false;

        public Instance.InstanceType instanceType = null;

        public long lastAccess = -1;

        public int valueCount = 1;

        public long timeout = 0; // for commit

        public long ttl = -1;

        public int index = -1;

        public TransactionRecord(String name, Object key, Data value) {
            this.name = name;
            this.key = key;
            this.value = value;
            this.getRecord = true;
            instanceType = ConcurrentMapManager.getInstanceType(name);
        }

        public TransactionRecord(String name, Object key, Data value, boolean newRecord) {
            this.name = name;
            this.key = key;
            this.value = value;
            this.newRecord = newRecord;
            instanceType = ConcurrentMapManager.getInstanceType(name);
        }

        public TransactionRecord(String name, Object key, Data value, int index, boolean newRecord) {
            this.name = name;
            this.key = key;
            this.value = value;
            this.newRecord = newRecord;
            this.index = index;
            instanceType = ConcurrentMapManager.getInstanceType(name);
        }

        public void commit() {
            if (instanceType == Instance.InstanceType.QUEUE) {
                commitQueue();
            } else {
                commitMap();
            }
        }

        public void commitMap() {
            if (removed) {
                if (instanceType.isSet()) {
                    factory.node.concurrentMapManager.new MRemoveItem().removeItem(name, key);
                } else if (!newRecord) {
                    if (instanceType.isMap()) {
                        factory.node.concurrentMapManager.new MRemove().remove(name, key);
                    } else if (instanceType.isMultiMap()) {
                        if (value == null) {
                            factory.node.concurrentMapManager.new MRemove().remove(name, key);
                        } else {
                            factory.node.concurrentMapManager.new MRemoveMulti().remove(name, key, value);
                        }
                    }
                }
                // since we do not have removeAndUnlock op, we should explicitly call unlock after remove!
                factory.node.concurrentMapManager.new MLock().unlock(name, key, -1);
            } else {
                if (instanceType.isMultiMap()) {
                    factory.node.concurrentMapManager.new MPutMulti().put(name, key, value);
                } else {
                    if(getRecord) {
                        factory.node.concurrentMapManager.new MLock().unlock(name, key, -1);
                    } else if (value != null) {
                        factory.node.concurrentMapManager.new MPut().putAfterCommit(name, key, value, ttl, id);
                    } else {
                        factory.node.concurrentMapManager.new MLock().unlock(name, key, -1);
                    }
                }
            }
        }

        public void commitQueue() {
            if (!removed) {
                factory.node.blockingQueueManager.offerCommit(name, key, value, index);
            }
        }

        public void rollback() {
            if (instanceType == Instance.InstanceType.QUEUE) {
                rollbackQueue();
            } else {
                rollbackMap();
            }
        }

        public void rollbackMap() {
            factory.node.concurrentMapManager.new MLock().unlock(name, key, -1);
        }

        public void rollbackQueue() {
            if (removed) {
                factory.node.blockingQueueManager.rollbackPoll(name, key);
            }
        }

        @Override
        public String toString() {
            return "TransactionRecord{" +
                    "instanceType=" + instanceType +
                    ", name='" + name + '\'' +
                    ", key=" + key +
                    ", value=" + value +
                    ", removed=" + removed +
                    ", newRecord=" + newRecord +
                    ", lastAccess=" + lastAccess +
                    ", valueCount=" + valueCount +
                    '}';
        }
    }
}
