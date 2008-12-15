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
import com.hazelcast.impl.BlockingQueueManager.Offer;
import com.hazelcast.impl.BlockingQueueManager.Poll;
import com.hazelcast.impl.ConcurrentMapManager.MGet;
import com.hazelcast.impl.ConcurrentMapManager.MLock;
import com.hazelcast.impl.ConcurrentMapManager.MPut;
import com.hazelcast.impl.ConcurrentMapManager.MRemove;
import com.hazelcast.nio.BuffersInputStream;
import com.hazelcast.nio.BuffersOutputStream;
import com.hazelcast.nio.InvocationQueue;
import com.hazelcast.nio.InvocationQueue.Data;
import com.hazelcast.nio.InvocationQueue.Invocation;
import com.hazelcast.nio.InvocationQueue.Serializer;
import com.hazelcast.nio.InvocationQueue.Invocation.DataBufferProvider;

public class ThreadContext {
	private final static ThreadLocal<ThreadContext> threadLocal = new ThreadLocal<ThreadContext>();

	private ThreadContext() {

	}

	public static ThreadContext get() {
		ThreadContext threadContext = (ThreadContext) threadLocal.get();
		if (threadContext == null) {
			threadContext = new ThreadContext();
			threadLocal.set(threadContext);
		}
		return threadContext;
	}

	long txnId = -1;

	TransactionImpl txn = null;

	ObjectReaderWriter objectReaderWriter = new ObjectReaderWriter();

	public long getTxnId() {
		return txnId;
	}

	public Data hardCopy(Data data) {
		if (data == null || data.size() == 0)
			return null;
		return objectReaderWriter.hardCopy(data);
	}

	public Data toData(Object obj) {
		try {
			return objectReaderWriter.writeObject(obj);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public Object toObject(Data data) {
		if (data == null || data.size() == 0)
			return null;
		return objectReaderWriter.readObject(data);
	}

	public ObjectReaderWriter getObjectReaderWriter() {
		return objectReaderWriter;
	}

	public void reset() {
		finalizeTxn();
	}

	public Transaction getTransaction() {
		if (txn == null) {
			txn = TransactionFactory.get().newTransaction();
			txnId = txn.getId();
		}
		return txn;
	}

	public void finalizeTxn() {
		txn = null;
		txnId = -1;
	}

	public Offer getOffer() {
		return BlockingQueueManager.get().new Offer();
	}

	public Poll getPoll() {
		return BlockingQueueManager.get().new Poll();
	}

	public MPut getMPut() {
		return ConcurrentMapManager.get().new MPut();
	}

	public MGet getMGet() {
		return ConcurrentMapManager.get().new MGet();
	}

	public MRemove getMRemove() {
		return ConcurrentMapManager.get().new MRemove();
	}

	public MLock getMLock() {
		return ConcurrentMapManager.get().new MLock();
	}

	class ObjectReaderWriter {
		private final Serializer serializer = new Serializer();

		private final Invocation invocation;

		private final DataBufferProvider bufferProvider;

		public ObjectReaderWriter() {
			invocation = InvocationQueue.instance().createNewInvocation();
			bufferProvider = invocation.dataBufferProvider;
			serializer.bbos.setBufferProvider(bufferProvider);
		}

		public BuffersInputStream getInputStream() {
			invocation.data.postRead();
			serializer.bbis.setBufferProvider(bufferProvider);
			return serializer.bbis;
		}

		public BuffersOutputStream getOutputStream() {
			return serializer.bbos;
		}

		public Data getCurrentData() {
			return invocation.doTake(invocation.data);
		}

		public Data writeObject(Object obj) throws Exception {
			if (obj instanceof Data)
				return (Data) obj;
			invocation.setNoData();
			serializer.writeObject(bufferProvider, obj);
			invocation.data.postRead();
			return invocation.doTake(invocation.data);
		}

		public Object readObject(Data data) {
			invocation.setNoData();
			invocation.doHardCopy(data, invocation.data);
			return serializer.readObject(bufferProvider);
		}

		public Data hardCopy(Data src) {
			return invocation.doHardCopy(src);
		}

		public void purge(Data data) {
			invocation.setNoData(data);
		}

	}
}
