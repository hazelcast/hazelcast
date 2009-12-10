/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.core.Transaction;
import com.hazelcast.core.Instance;
import com.hazelcast.impl.BlockingQueueManager.CommitPoll;
import com.hazelcast.impl.BlockingQueueManager.Offer;
import com.hazelcast.impl.FactoryImpl.MProxy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

class TransactionImpl implements Transaction {

    protected static Logger logger = Logger.getLogger(TransactionImpl.class.getName());

    private final long id;
    private final FactoryImpl factory;

    List<TransactionRecord> transactionRecords = new ArrayList<TransactionRecord>(1);

    private int status = TXN_STATUS_NO_TXN;

    public TransactionImpl(FactoryImpl factory, long txnId) {
        this.id = txnId;
        this.factory = factory;
    }

    public Object attachAddOp(final String name, final Object key) {
        TransactionRecord rec = findTransactionRecord(name, key);
        if (rec == null) {
            rec = new TransactionRecord(name, key, 1, true);
            transactionRecords.add(rec);
            return null;
        } else {
            final Object old = rec.value;
            rec.value = ((Integer) rec.value) + 1;
            rec.removed = false;
            return old;
        }
    }

    public Object attachPutOp(final String name, final Object key, final Object value,
                              final boolean newRecord) {
        TransactionRecord rec = findTransactionRecord(name, key);
        if (rec == null) {
            rec = new TransactionRecord(name, key, value, newRecord);
            transactionRecords.add(rec);
            return null;
        } else {
            final Object old = rec.value;
            rec.value = value;
            rec.removed = false;
            return old;
        }
    }

    public Object attachRemoveOp(final String name, final Object key, final Object value,
                                 final boolean newRecord) {
        TransactionRecord rec = findTransactionRecord(name, key);
        Object oldValue = null;
        if (rec == null) {
            rec = new TransactionRecord(name, key, value, newRecord);
            transactionRecords.add(rec);
        } else {
            oldValue = rec.value;
            rec.value = value;
        }
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
            for (final TransactionRecord transactionRecord : transactionRecords) {
                transactionRecord.commit();
            }
        } catch (final Exception e) {
            e.printStackTrace();
        } finally {
            finalizeTxn();
            status = TXN_STATUS_COMMITTED;
        }
    }

    public boolean containsValue(final String name, final Object value) {
        for (final TransactionRecord transactionRecord : transactionRecords) {
            if (transactionRecord.name.equals(name)) {
                if (!transactionRecord.removed) {
                    if (value.equals(transactionRecord.value))
                        return true;
                }
            }
        }
        return false;
    }

    public TransactionRecord findTransactionRecord(final String name, final Object key) {
        for (final TransactionRecord transactionRecord : transactionRecords) {
            if (transactionRecord.name.equals(name)) {
                if (transactionRecord.key != null) {
                    if (transactionRecord.key.equals(key))
                        return transactionRecord;
                }
            }
        }
        return null;
    }

    public Object get(final String name, final Object key) {
        final TransactionRecord rec = findTransactionRecord(name, key);
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

    public boolean has(final String name, final Object key) {
        final TransactionRecord rec = findTransactionRecord(name, key);
        return rec != null;
    }

    public boolean isNew(final String name, final Object key) {
        final TransactionRecord rec = findTransactionRecord(name, key);
        return (rec != null && !rec.removed && rec.newRecord);
    }

    public boolean isRemoved(final String name, final Object key) {
        final TransactionRecord rec = findTransactionRecord(name, key);
        return (rec != null && rec.removed);
    }

    public void rollback() throws IllegalStateException {
        if (status == TXN_STATUS_NO_TXN || status == TXN_STATUS_UNKNOWN
                || status == TXN_STATUS_COMMITTED || status == TXN_STATUS_ROLLED_BACK)
            throw new IllegalStateException("Transaction is not ready to rollback. Status= "
                    + status);
        status = TXN_STATUS_ROLLING_BACK;
        try {
            for (final TransactionRecord transactionRecord : transactionRecords) {
                transactionRecord.rollback();
            }
        } catch (final Exception e) {
            e.printStackTrace();
        } finally {
            finalizeTxn();
            status = TXN_STATUS_ROLLED_BACK;
        }
    }

    public int size(final String name) {
        int size = 0;
        for (final TransactionRecord transactionRecord : transactionRecords) {
            if (transactionRecord.name.equals(name)) {
                if (transactionRecord.removed) {
                    if (transactionRecord.name.startsWith("m:s:")) {
                        size--;
                    } else if (!transactionRecord.newRecord) {
                        if (transactionRecord.instanceType != Instance.InstanceType.QUEUE) {
                            size--;
                        }
                    }
                } else {
                    if (transactionRecord.newRecord) {
                        size++;
                    }
                }
            }
        }
        return size;
    }

    public List newValues(final String name) {
        List lsValues = null;
        for (final TransactionRecord transactionRecord : transactionRecords) {
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

    public List<Map.Entry> newEntries(final String name) {
        List<Map.Entry> lsEntries = null;
        for (final TransactionRecord transactionRecord : transactionRecords) {
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

        public TransactionRecord(final String name, final Object key, final Object value,
                                 final boolean newRecord) {
            this.name = name;
            this.key = key;
            this.value = value;
            this.newRecord = newRecord;
            instanceType = factory.node.concurrentMapManager.getInstanceType(name);
        }

        public void commit() {
            if (instanceType == Instance.InstanceType.QUEUE)
                commitQueue();
            else
                commitMap();
        }

        public void commitMap() {
            if (removed) {
                if (name.startsWith("m:s:")) {
                    ConcurrentMapManager.MRemoveItem mRemoveItem = factory.node.concurrentMapManager.new MRemoveItem();
                    mRemoveItem.removeItem(name, key);
                } else if (!newRecord) {
                    factory.node.concurrentMapManager.new MRemove().remove(name, key, -1);
                } else {
                    factory.node.concurrentMapManager.new MLock().unlock(name, key, -1);
                }
            } else {
                if (instanceType == Instance.InstanceType.LIST) {
                    int count = (Integer) value;
                    for (int i=0; i < count; i++) {
                        factory.node.concurrentMapManager.new MAdd().addToList(name, key);
                    }
                } else {
                    factory.node.concurrentMapManager.new MPut().put(name, key, value, -1,  -1);
                }
            }
        }

        public void commitQueue() {
            if (removed) {
                commitPoll();
                // remove the backup at the next member
            } else {
                offerAgain();
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
            final Object proxy = factory.getOrCreateProxyByName(name);
            if (proxy instanceof MProxy) {
                mapProxy = (MProxy) proxy;
            }
            if (mapProxy != null) mapProxy.unlock(key);
        }

        public void rollbackQueue() {
            if (removed) {
                offerAgain();
                // if offer fails, no worries.
                // there is a backup at the next member
            }
        }

        private void commitPoll() {
            final CommitPoll commitPoll = factory.node.blockingQueueManager.new CommitPoll();
            commitPoll.commitPoll(name);
        }

        private void offerAgain() {
            final Offer offer = factory.node.blockingQueueManager.new Offer();
            offer.offer(name, value, 0, false);
        }
    }
}
