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

package com.hazelcast.core;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;

public final class Hazelcast {

	private Hazelcast() {

	}

	public static <E> IQueue<E> getQueue(String name) {
		return com.hazelcast.impl.FactoryImpl.getQueue(name);
	}

	public static <E> ITopic<E> getTopic(String name) {
		return com.hazelcast.impl.FactoryImpl.getTopic(name);
	}

	public static <E> ISet<E> getSet(String name) {
		return com.hazelcast.impl.FactoryImpl.getSet(name);
	}

	public static <E> IList<E> getList(String name) {
		return com.hazelcast.impl.FactoryImpl.getList(name);
	}

	public static <K, V> IMap<K, V> getMap(String name) {
		return com.hazelcast.impl.FactoryImpl.getMap(name);
	}

	public static <K, V> MultiMap<K, V> getMultiMap(String name) {
		return com.hazelcast.impl.FactoryImpl.getMultiMap(name);
	}

	public static Lock getLock(Object obj) {
		return com.hazelcast.impl.FactoryImpl.getLock(obj);
	}

	public static Cluster getCluster() {
		return com.hazelcast.impl.FactoryImpl.getCluster();
	}

	public static ExecutorService getExecutorService() {
		return com.hazelcast.impl.FactoryImpl.getExecutorService();
	}

	public static Transaction getTransaction() {
		return com.hazelcast.impl.FactoryImpl.getTransaction();
	}

	public static IdGenerator getIdGenerator(String name) {
		return com.hazelcast.impl.FactoryImpl.getIdGenerator(name);
	}
}
