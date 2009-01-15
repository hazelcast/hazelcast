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

import com.hazelcast.core.Transaction;
import com.hazelcast.impl.BlockingQueueManager.CommitPoll;
import com.hazelcast.impl.BlockingQueueManager.Offer;
import com.hazelcast.impl.FactoryImpl.CProxy;
import com.hazelcast.impl.FactoryImpl.CollectionProxy;

class TransactionImpl implements Transaction, Constants {

	private final long id;

	List<TxnRecord> lsTxnRecords = new ArrayList<TxnRecord>(1);

	private int status = TXN_STATUS_NO_TXN;

	public TransactionImpl(long txnId) {
		this.id = txnId;
	}

	public int size(String name) {
		int size = 0;
		for (TxnRecord txnRecord : lsTxnRecords) {
			if (txnRecord.name.equals(name)) {
				if (txnRecord.removed) {
					if (!txnRecord.newRecord) {
						if (txnRecord.map)
							size--;
					}
				} else {
					size++;
				}
			}
		}
		return size;
	}

	public TxnRecord findTxnRecord(String name, Object key) {
		for (TxnRecord txnRecord : lsTxnRecords) {
			if (txnRecord.name.equals(name)) {
				if (txnRecord.key != null) {
					if (txnRecord.key.equals(key))
						return txnRecord;
				}
			}
		}
		return null;
	}

	public Object attachPutOp(String name, Object key, Object value, boolean newRecord) {
		TxnRecord rec = findTxnRecord(name, key);
		if (rec == null) {
			rec = new TxnRecord(name, key, value, newRecord);
			lsTxnRecords.add(rec);
			return null;
		} else {
			Object old = rec.value;
			rec.value = value;
			rec.removed = false;
			return old;
		}
	}

	public Object attachRemoveOp(String name, Object key, Object value, boolean newRecord) {
		TxnRecord rec = findTxnRecord(name, key);
		Object oldValue = null;
		if (rec == null) {
			rec = new TxnRecord(name, key, value, newRecord);
			rec.removed = true;
			lsTxnRecords.add(rec);
			return null;
		} else {
			oldValue = rec.value;
			rec.value = value;
		}
		rec.removed = true;
		return oldValue;
	}

	public boolean has(String name, Object key) {
		TxnRecord rec = findTxnRecord(name, key);
		if (rec == null)
			return false;
		return true;
	}

	public Object get(String name, Object key) {
		TxnRecord rec = findTxnRecord(name, key);
		if (rec == null)
			return null;
		if (rec.removed)
			return null;
		return rec.value;
	}

	public boolean containsValue(String name, Object value) {
		for (TxnRecord txnRecord : lsTxnRecords) {
			if (txnRecord.name.equals(name)) {
				if (!txnRecord.removed) {
					if (value.equals(txnRecord.value))
						return true;
				}
			}
		}
		return false;
	}

	public class TxnRecord {
		public String name;
		public Object key;
		public Object value;
		public boolean removed = false;
		public boolean newRecord = false;
		public boolean map = true;

		public TxnRecord(String name, Object key, Object value, boolean newRecord) {
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

		public void rollback() {
			if (map)
				rollbackMap();
			else
				rollbackQueue();
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

		public void rollbackMap() {
			CProxy mapProxy = null;
			Object proxy = FactoryImpl.getProxy(name);
			if (proxy instanceof CProxy) {
				mapProxy = (CProxy) proxy;
			} else if (proxy instanceof CollectionProxy) {
				mapProxy = ((CollectionProxy) proxy).getCProxy();
			}
			mapProxy.unlock(key);
		}

		public void commitQueue() {
			if (removed) {
				commitPoll();
				// remove the backup at the next member
			} else {
				offerAgain();
			}
		}

		public void rollbackQueue() {
			if (removed) {
				offerAgain();
				// if offer fails, no worries.
				// there is a backup at the next member
			}
		}

		private void offerAgain() {
			Offer offer = ThreadContext.get().getOffer();
			offer.offer(name, value, 0, -1);
		}

		private void commitPoll() {
			CommitPoll commitPoll = BlockingQueueManager.get().new CommitPoll();
			commitPoll.commitPoll(name);
		}
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
			for (TxnRecord txnRecord : lsTxnRecords) {
				txnRecord.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			finalizeTxn();
			status = TXN_STATUS_COMMITTED;
		}
	}

	public void rollback() throws IllegalStateException {
		if (status == TXN_STATUS_NO_TXN || status == TXN_STATUS_UNKNOWN
				|| status == TXN_STATUS_COMMITTED || status == TXN_STATUS_ROLLED_BACK)
			throw new IllegalStateException("Transaction is not ready to rollback. Status= "
					+ status);
		status = TXN_STATUS_ROLLING_BACK;
		try {
			for (TxnRecord txnRecord : lsTxnRecords) {
				txnRecord.rollback();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			finalizeTxn();
			status = TXN_STATUS_ROLLED_BACK;
		}
	}

	private void finalizeTxn() {
		lsTxnRecords.clear();
		status = TXN_STATUS_NO_TXN;
		ThreadContext.get().finalizeTxn();
	}

	public int getStatus() {
		return status;
	}

	public static void main(String[] args) {
		System.out.println(Long.MAX_VALUE / 1000000000);
	}

	public long getId() {
		return id;
	}

	@Override
	public String toString() {
		return "TransactionImpl [" + id + "]";
	}
}
