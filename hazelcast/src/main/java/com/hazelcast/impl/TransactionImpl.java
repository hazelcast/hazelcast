/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.core.Instance;
import com.hazelcast.core.Transaction;
import com.hazelcast.impl.BlockingQueueManager.CommitPoll;
import com.hazelcast.impl.BlockingQueueManager.Offer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class TransactionImpl implements Transaction {

    private final long id;
    private final FactoryImpl factory;

    List<TransactionRecord> transactionRecords = new ArrayList<TransactionRecord>(1);

    private int status = TXN_STATUS_NO_TXN;

    public TransactionImpl(FactoryImpl factory, long txnId) {
        this.id = txnId;
        this.factory = factory;
    }

    public Object attachAddOp(String name, Object key) {
        TransactionRecord rec = findTransactionRecord(name, key);
        if (rec == null) {
            rec = new TransactionRecord(name, key, 1, true);
            transactionRecords.add(rec);
            return null;
        } else {
            Object old = rec.value;
            rec.value = ((Integer) rec.value) + 1;
            rec.removed = false;
            return old;
        }
    }

    public Object attachPutOp(String name, Object key, Object value, boolean newRecord) {
        return attachPutOp(name, key, value, 0, -1, newRecord);
    }

    public Object attachPutOp(String name, Object key, Object value, long timeout, boolean newRecord) {
        return attachPutOp(name, key, value, timeout, -1, newRecord);
    }

    public Object attachPutOp(String name, Object key, Object value, long timeout, long ttl, boolean newRecord) {
        Instance.InstanceType instanceType = ConcurrentMapManager.getInstanceType(name);
        Object matchValue = (instanceType.isMultiMap()) ? value : null;
        TransactionRecord rec = findTransactionRecord(name, key, matchValue);
        if (rec == null) {
            rec = new TransactionRecord(name, key, value, newRecord);
            rec.timeout = timeout;
            rec.ttl = ttl;
            transactionRecords.add(rec);
            return null;
        } else {
            Object old = rec.value;
            rec.value = value;
            rec.removed = false;
            return old;
        }
    }

    public Object attachRemoveOp(String name, Object key, Object value, boolean newRecord) {
        return attachRemoveOp(name, key, value, newRecord, 1);
    }

    public Object attachRemoveOp(String name, Object key, Object value, boolean newRecord, int valueCount) {
        Instance.InstanceType instanceType = ConcurrentMapManager.getInstanceType(name);
        Object matchValue = (instanceType.isMultiMap()) ? value : null;
        TransactionRecord rec = findTransactionRecord(name, key, matchValue);
        Object oldValue = null;
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
        if (status == TXN_STATUS_ACTIVE)
            throw new IllegalStateException("Transaction is already active");
        status = TXN_STATUS_ACTIVE;
    }

    public void commit() throws IllegalStateException {
        if (status != TXN_STATUS_ACTIVE)
            throw new IllegalStateException("Transaction is not active");
        status = TXN_STATUS_COMMITTING;
        try {
            for (TransactionRecord transactionRecord : transactionRecords) {
                transactionRecord.commit();
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

    public boolean containsValue(String name, Object value) {
        for (TransactionRecord transactionRecord : transactionRecords) {
            if (transactionRecord.name.equals(name)) {
                if (!transactionRecord.removed) {
                    if (value.equals(transactionRecord.value))
                        return true;
                }
            }
        }
        return false;
    }

    public TransactionRecord findTransactionRecord(String name, Object key) {
        return findTransactionRecord(name, key, null);
    }

    public TransactionRecord findTransactionRecord(String name, Object key, Object value) {
        for (TransactionRecord transactionRecord : transactionRecords) {
            if (transactionRecord.name.equals(name)) {
                if (transactionRecord.key != null) {
                    if (transactionRecord.key.equals(key)) {
                        if (value == null || value.equals(transactionRecord.value)) {
                            return transactionRecord;
                        }
                    }
                }
            }
        }
        return null;
    }

    public Object get(String name, Object key) {
        TransactionRecord rec = findTransactionRecord(name, key);
        if (rec == null)
            return null;
        if (rec.removed)
            return null;
        rec.lastAccess = System.currentTimeMillis();
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

    public void rollback() throws IllegalStateException {
        if (status == TXN_STATUS_NO_TXN || status == TXN_STATUS_UNKNOWN
                || status == TXN_STATUS_COMMITTED || status == TXN_STATUS_ROLLED_BACK)
            throw new IllegalStateException("Transaction is not ready to rollback. Status= "
                    + status);
        status = TXN_STATUS_ROLLING_BACK;
        try {
            for (TransactionRecord transactionRecord : transactionRecords) {
                transactionRecord.rollback();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            finalizeTxn();
            status = TXN_STATUS_ROLLED_BACK;
        }
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
                        size += (Integer) transactionRecord.value;
                    } else {
                        size++;
                    }
                }
            }
        }
        return size;
    }

    public List newValues(String name) {
        List lsValues = null;
        for (TransactionRecord transactionRecord : transactionRecords) {
            if (transactionRecord.name.equals(name)) {
                if (!transactionRecord.removed) {
                    if (transactionRecord.value != null) {
                        if (transactionRecord.newRecord) {
                            if (lsValues == null) {
                                lsValues = new ArrayList(2);
                            }
                            lsValues.add(transactionRecord.value);
                        }
                    }
                }
            }
        }
        return lsValues;
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
                            lsEntries.add(BaseManager.createSimpleEntry(factory, name, transactionRecord.key, transactionRecord.value));
                        }
                    }
                }
            }
        }
        return lsEntries;
    }

    @Override
    public String toString() {
        return "TransactionImpl [" + id + "]";
    }

    private void finalizeTxn() {
        transactionRecords.clear();
        status = TXN_STATUS_NO_TXN;
        ThreadContext.get().finalizeTxn();
    }

    private class TransactionRecord {
        public String name;

        public Object key;

        public Object value;

        public boolean removed = false;

        public boolean newRecord = false;

        public Instance.InstanceType instanceType = null;

        public long lastAccess = -1;

        public int valueCount = 1;

        public long timeout = 0; // for commit

        public long ttl = -1;

        public TransactionRecord(String name, Object key, Object value, boolean newRecord) {
            this.name = name;
            this.key = key;
            this.value = value;
            this.newRecord = newRecord;
            instanceType = ConcurrentMapManager.getInstanceType(name);
        }

        public void commit() {
            if (instanceType == Instance.InstanceType.QUEUE)
                commitQueue();
            else
                commitMap();
        }

        public void commitMap() {
            if (removed) {
                if (instanceType.isSet()) {
                    ConcurrentMapManager.MRemoveItem mRemoveItem = factory.node.concurrentMapManager.new MRemoveItem();
                    mRemoveItem.removeItem(name, key);
                } else if (!newRecord) {
                    if (instanceType.isMap()) {
                        factory.node.concurrentMapManager.new MRemove().remove(name, key, -1);
                    } else if (instanceType.isMultiMap()) {
                        factory.node.concurrentMapManager.new MRemoveMulti().remove(name, key, value);
                    }
                } else {
                    factory.node.concurrentMapManager.new MLock().unlock(name, key, -1);
                }
            } else {
                if (instanceType.isList()) {
                    int count = (Integer) value;
                    for (int i = 0; i < count; i++) {
                        factory.node.concurrentMapManager.new MAdd().addToList(name, key);
                    }
                } else if (instanceType.isMultiMap()) {
                    factory.node.concurrentMapManager.new MPutMulti().put(name, key, value);
                } else {
                    factory.node.concurrentMapManager.new MPut().put(name, key, value, -1, ttl);
                }
            }
        }

        public void commitQueue() {
            if (removed) {
                commitPoll();
                // remove the backup at the next member
            } else {
                offerAgain(false);
            }
        }

        public void rollback() {
            if (instanceType == Instance.InstanceType.QUEUE)
                rollbackQueue();
            else
                rollbackMap();
        }

        public void rollbackMap() {
            MProxy mapProxy = null;
            Object proxy = factory.getOrCreateProxyByName(name);
            if (proxy instanceof MProxy) {
                mapProxy = (MProxy) proxy;
            }
            if (mapProxy != null) mapProxy.unlock(key);
        }

        public void rollbackQueue() {
            if (removed) {
                offerAgain(true);
                // if offer fails, no worries.
                // there is a backup at the next member
            }
        }

        private void commitPoll() {
            CommitPoll commitPoll = factory.node.blockingQueueManager.new CommitPoll();
            commitPoll.commitPoll(name);
        }

        private void offerAgain(boolean first) {
            Offer offer = null;
            if (first) {
                offer = factory.node.blockingQueueManager.new OfferFirst();
            } else {
                offer = factory.node.blockingQueueManager.new Offer();
            }
            try {
                boolean offered = offer.offer(name, value, timeout, false);
                if (!offered) {
                    throw new RuntimeException("Failed to offer in " + timeout + " ms.");
                }
            } catch (InterruptedException ignored) {
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
