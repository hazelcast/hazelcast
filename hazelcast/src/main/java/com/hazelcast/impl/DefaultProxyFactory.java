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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import com.hazelcast.impl.FactoryImpl.ProxyKey;

public class DefaultProxyFactory implements ProxyFactory {
	
	private final FactoryImpl factory;
	private final TransactionFactory transactionFactory;

	public DefaultProxyFactory(FactoryImpl factory) {
		super();
		this.factory = factory;
		this.transactionFactory = new TransactionFactory(factory);
	}

	public MProxy createMapProxy(String name) {
		return new MProxyImpl(name, factory);
	}

	public QProxy createQueueProxy(String name) {
		return new QProxyImpl(name, factory);
	}

	public TopicProxy createTopicProxy(String name) {
		return new TopicProxyImpl(name, factory);
	}

	public MultiMapProxy createMultiMapProxy(String name) {
		return new MultiMapProxyImpl(name, factory);
	}

	public ListProxy createListProxy(String name) {
		return new ListProxyImpl(name, factory);
	}

	public SetProxy createSetProxy(String name) {
		return new SetProxyImpl(name, factory);
	}

	public LockProxy createLockProxy(Object key) {
		return new LockProxyImpl(factory, key);
	}

	public AtomicNumberProxy createAtomicNumberProxy(String name) {
		return new AtomicNumberProxyImpl(name, factory);
	}

	public SemaphoreProxy createSemaphoreProxy(String name) {
		return new SemaphoreProxyImpl(name, factory);
	}

	public CountDownLatchProxy createCountDownLatchProxy(String name) {
		return new CountDownLatchProxyImpl(name, factory);
	}

	public IdGeneratorProxy createIdGeneratorProxy(String name) {
		return new IdGeneratorProxy(name, factory);
	}
	
	public ExecutorService createExecutorServiceProxy(String name) {
		return new ExecutorServiceProxy(factory.node, name);
	}

	public TransactionImpl createTransaction() {
		return transactionFactory.newTransaction();
	}
	
	public void checkProxy(ProxyKey proxyKey) {
	}
	
	private class TransactionFactory {
	    AtomicLong ids = new AtomicLong(0);
	    final FactoryImpl factory;

	    TransactionFactory(FactoryImpl factory) {
	        this.factory = factory;
	    }

	    public TransactionImpl newTransaction() {
	        return new TransactionImpl(factory, newTransactionId());
	    }

	    public long newTransactionId() {
	        return ids.incrementAndGet();
	    }
	}
}
