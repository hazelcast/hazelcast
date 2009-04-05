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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hazelcast.core.Transaction;
import com.hazelcast.impl.BaseManager.SimpleDataEntry;
import com.hazelcast.impl.BlockingQueueManager.CommitPoll;
import com.hazelcast.impl.BlockingQueueManager.Offer;
import com.hazelcast.impl.FactoryImpl.CollectionProxy;
import com.hazelcast.impl.FactoryImpl.MProxy;

class TransactionImpl implements Transaction, Constants {

    public class TxnRecord {
        public String name;

        public Object key;

        public Object value;

        public boolean removed = false;

        public boolean newRecord = false;

        public boolean map = true;

        public TxnRecord(final String name, final Object key, final Object value,
                         final boolean newRecord) {
            this.name = name;
            this.key = key;
            this.value = value;
            this.newRecord = newRecord;
            if (name.startsWith("q:"))
                map = false;
        }

        public void commit() {
            if (map)
                commitMap();
            else
                commitQueue();
        }

        public void commitMap() {
            if (removed) {
                if (!newRecord) {
                    ThreadContext.get().getMRemove().remove(name, key, -1, -1);
                }
            } else {
                ThreadContext.get().getMPut().put(name, key, value, -1, -1);
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
            if (map)
                rollbackMap();
            else
                rollbackQueue();
        }

        public void rollbackMap() {
            MProxy mapProxy = null;
            final Object proxy = FactoryImpl.getProxy(name);
            if (proxy instanceof MProxy) {
                mapProxy = (MProxy) proxy;
            } else if (proxy instanceof CollectionProxy) {
                mapProxy = ((CollectionProxy) proxy).getCProxy();
            }
            mapProxy.unlock(key);
        }

        public void rollbackQueue() {
            if (removed) {
                offerAgain();
                // if offer fails, no worries.
                // there is a backup at the next member
            }
        }

        private void commitPoll() {
            final CommitPoll commitPoll = BlockingQueueManager.get().new CommitPoll();
            commitPoll.commitPoll(name);
        }

        private void offerAgain() {
            final Offer offer = ThreadContext.get().getOffer();
            offer.offer(name, value, 0, -1);
        }
    }

    protected static Logger logger = Logger.getLogger(TransactionImpl.class.getName());

    private final long id;

    List<TxnRecord> lsTxnRecords = new ArrayList<TxnRecord>(1);

    private int status = TXN_STATUS_NO_TXN;

    public TransactionImpl(final long txnId) {
        this.id = txnId;
    }

    public static void main(final String[] args) {
        logger.log(Level.INFO, new Long(Long.MAX_VALUE / 1000000000).toString());
    }

    public Object attachPutOp(final String name, final Object key, final Object value,
                              final boolean newRecord) {
        TxnRecord rec = findTxnRecord(name, key);
        if (rec == null) {
            rec = new TxnRecord(name, key, value, newRecord);
            lsTxnRecords.add(rec);
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
        TxnRecord rec = findTxnRecord(name, key);
        Object oldValue = null;
        if (rec == null) {
            rec = new TxnRecord(name, key, value, newRecord);
            rec.removed = true;
            lsTxnRecords.add(rec);
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
            for (final TxnRecord txnRecord : lsTxnRecords) {
                txnRecord.commit();
            }
        } catch (final Exception e) {
            e.printStackTrace();
        } finally {
            finalizeTxn();
            status = TXN_STATUS_COMMITTED;
        }
    }

    public boolean containsValue(final String name, final Object value) {
        for (final TxnRecord txnRecord : lsTxnRecords) {
            if (txnRecord.name.equals(name)) {
                if (!txnRecord.removed) {
                    if (value.equals(txnRecord.value))
                        return true;
                }
            }
        }
        return false;
    }

    public TxnRecord findTxnRecord(final String name, final Object key) {
        for (final TxnRecord txnRecord : lsTxnRecords) {
            if (txnRecord.name.equals(name)) {
                if (txnRecord.key != null) {
                    if (txnRecord.key.equals(key))
                        return txnRecord;
                }
            }
        }
        return null;
    }

    public Object get(final String name, final Object key) {
        final TxnRecord rec = findTxnRecord(name, key);
        if (rec == null)
            return null;
        if (rec.removed)
            return null;
        return rec.value;
    }

    public long getId() {
        return id;
    }

    public int getStatus() {
        return status;
    }

    public boolean has(final String name, final Object key) {
        final TxnRecord rec = findTxnRecord(name, key);
        if (rec == null)
            return false;
        return true;
    }

    public void rollback() throws IllegalStateException {
        if (status == TXN_STATUS_NO_TXN || status == TXN_STATUS_UNKNOWN
                || status == TXN_STATUS_COMMITTED || status == TXN_STATUS_ROLLED_BACK)
            throw new IllegalStateException("Transaction is not ready to rollback. Status= "
                    + status);
        status = TXN_STATUS_ROLLING_BACK;
        try {
            for (final TxnRecord txnRecord : lsTxnRecords) {
                txnRecord.rollback();
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
        for (final TxnRecord txnRecord : lsTxnRecords) {
            if (txnRecord.name.equals(name)) {
                if (txnRecord.removed) {
                    if (!txnRecord.newRecord) {
                        if (txnRecord.map) {
                            size--;
                        }
                    }
                } else {
                    size++;
                }
            }
        }
        return size;
    }

    public List<SimpleDataEntry> entries(final String name) {
        List<SimpleDataEntry> lsEntries = null;
        for (final TxnRecord txnRecord : lsTxnRecords) {
            if (txnRecord.name.equals(name)) {
                if (!txnRecord.removed) {
                    if (txnRecord.value != null) {
                        if (lsEntries == null) {
                            lsEntries = new ArrayList<SimpleDataEntry>(2);
                        }
                        lsEntries.add(new SimpleDataEntry(name, txnRecord.key, txnRecord.value));
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
        lsTxnRecords.clear();
        status = TXN_STATUS_NO_TXN;
        ThreadContext.get().finalizeTxn();
    }
}
