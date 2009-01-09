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

import static com.hazelcast.impl.Constants.MapTypes.MAP_TYPE_LIST;
import static com.hazelcast.impl.Constants.MapTypes.MAP_TYPE_MAP;
import static com.hazelcast.impl.Constants.MapTypes.MAP_TYPE_SET;

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractQueue;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ICollection;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.MessageListener;
import com.hazelcast.core.Transaction;
import com.hazelcast.impl.BaseManager.Processable;
import com.hazelcast.impl.BlockingQueueManager.Offer;
import com.hazelcast.impl.BlockingQueueManager.Poll;
import com.hazelcast.impl.BlockingQueueManager.QIterator;
import com.hazelcast.impl.BlockingQueueManager.Size;
import com.hazelcast.impl.ClusterManager.CreateProxy;
import com.hazelcast.impl.ConcurrentMapManager.MAdd;
import com.hazelcast.impl.ConcurrentMapManager.MContainsKey;
import com.hazelcast.impl.ConcurrentMapManager.MContainsValue;
import com.hazelcast.impl.ConcurrentMapManager.MGet;
import com.hazelcast.impl.ConcurrentMapManager.MIterator;
import com.hazelcast.impl.ConcurrentMapManager.MLock;
import com.hazelcast.impl.ConcurrentMapManager.MPut;
import com.hazelcast.impl.ConcurrentMapManager.MRemove;
import com.hazelcast.impl.ConcurrentMapManager.MSize;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.InvocationQueue.Data;

public class FactoryImpl implements Constants {

	private static ConcurrentMap proxies = new ConcurrentHashMap(1000);

	private static CProxy lockCProxy = new CProxy("m:locxtz3");

	private static ConcurrentMap mapLockProxies = new ConcurrentHashMap(100);

	private static ConcurrentMap mapUUIDs = new ConcurrentHashMap(100);

	static final ExecutorServiceProxy executorServiceImpl = new ExecutorServiceProxy();

	private static Node node = null;

	static AtomicBoolean inited = new AtomicBoolean(false);

	static void init() {

		if (!inited.get()) {
			synchronized (Node.class) {
				if (!inited.get()) {
					node = Node.get();
					node.start();
					inited.set(true);
				}
			}
		}
	}

	static IdGeneratorImpl getUUID(String name) {
		IdGeneratorImpl uuid = (IdGeneratorImpl) mapUUIDs.get(name);
		if (uuid != null)
			return uuid;
		synchronized (IdGeneratorImpl.class) {
			uuid = new IdGeneratorImpl(name);
			IdGeneratorImpl old = (IdGeneratorImpl) mapUUIDs.putIfAbsent(name, uuid);
			if (old != null)
				uuid = old;
		}
		return uuid;
	}

	static Collection getProxies() {
		if (!inited.get())
			init();
		return proxies.values();
	}

	public static ExecutorService getExecutorService() {
		if (!inited.get())
			init();
		return executorServiceImpl;
	}

	public static ClusterImpl getCluster() {
		if (!inited.get())
			init();
		return node.getClusterImpl();
	}

	public static IdGenerator getIdGenerator(String name) {
		if (!inited.get())
			init();
		return getUUID(name);
	}

	public static <K, V> IMap<K, V> getMap(String name) {
		name = "c:" + name;
		return (IMap<K, V>) getProxy(name);
	}

	public static <E> IQueue<E> getQueue(String name) {
		name = "q:" + name;
		return (IQueue) getProxy(name);
	}

	public static <E> ITopic<E> getTopic(String name) {
		name = "t:" + name;
		return (ITopic) getProxy(name);
	}

	public static <E> ISet<E> getSet(String name) {
		name = "m:s:" + name;
		return (ISet) getProxy(name);
	}

	public static <E> IList<E> getList(String name) {
		name = "m:l:" + name;
		return (IList) getProxy(name);
	}

	public static Transaction getTransaction() {
		if (!inited.get())
			init();
		ThreadContext threadContext = ThreadContext.get();
		Transaction txn = threadContext.txn;
		if (txn == null)
			txn = threadContext.getTransaction();
		return txn;
	}

	public static Lock getLock(Object key) {
		if (!inited.get())
			init();
		LockProxy lockProxy = (LockProxy) mapLockProxies.get(key);
		if (lockProxy == null) {
			lockProxy = new LockProxy(lockCProxy, key);
			mapLockProxies.put(key, lockProxy);
		}
		return lockProxy;
	}

	public static Object getProxy(final String name) {
		if (!inited.get())
			init();
		Object proxy = proxies.get(name);
		if (proxy == null) {
			CreateProxyProcess createProxyProcess = new CreateProxyProcess(name);
			synchronized (createProxyProcess) {
				ClusterService.get().enqueueAndReturn(createProxyProcess);
				try {
					createProxyProcess.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			proxy = createProxyProcess.getProxy();
		}
		return proxy;
	}

	// should only be called from service thread!!
	static Object createProxy(String name) {
		Object proxy = proxies.get(name);
		if (proxy == null) {
			if (name.startsWith("q:")) {
				proxy = proxies.get(name);
				if (proxy == null) {
					proxy = new BProxy(name);
					proxies.put(name, proxy);
				}
			} else if (name.startsWith("t:")) {
				proxy = proxies.get(name);
				if (proxy == null) {
					proxy = new TopicProxy(name);
					proxies.put(name, proxy);
				}
			} else if (name.startsWith("c:")) {
				proxy = proxies.get(name);
				if (proxy == null) {
					proxy = new CProxy(name);
					proxies.put(name, proxy);
				}
			} else if (name.startsWith("m:")) {
				proxy = proxies.get(name);
				if (proxy == null) {
					byte mapType = BaseManager.getMapType(name);
					CProxy mapProxy = new CProxy(name);
					if (mapType == MAP_TYPE_SET) {
						proxy = new SetProxy(mapProxy);
					} else if (mapType == MAP_TYPE_LIST) {
						proxy = new ListProxy(mapProxy);
					} else {
						proxy = mapProxy;
					}
					proxies.put(name, proxy);
				}
			}
		}
		return proxy;
	}

	static class LockProxy implements java.util.concurrent.locks.Lock {

		CProxy mapProxy = null;

		Object key = null;

		public LockProxy(CProxy mapProxy, Object key) {
			super();
			this.mapProxy = mapProxy;
			this.key = key;
		}

		public void lock() {
			mapProxy.lock(key);
		}

		public void lockInterruptibly() throws InterruptedException {
		}

		public Condition newCondition() {
			return null;
		}

		public boolean tryLock() {
			return mapProxy.tryLock(key);
		}

		public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
			return mapProxy.tryLock(key, time, unit);
		}

		public void unlock() {
			mapProxy.unlock(key);
		}
	}

	private static class CreateProxyProcess implements Processable, Constants {
		String name;

		Object proxy = null;

		public CreateProxyProcess(String name) {
			super();
			this.name = name;
		}

		public void process() {
			proxy = createProxy(name);
			ClusterManager.get().sendProcessableToAll(new CreateProxy(name), false);
			synchronized (CreateProxyProcess.this) {
				CreateProxyProcess.this.notify();
			}
		}

		public Object getProxy() {
			return proxy;
		}
	}

	static class TopicProxy implements ITopic {
		String name;

		public TopicProxy(String name) {
			super();
			this.name = name;
		}

		public void publish(Object msg) {
			TopicManager.get().doSend(name, msg);
		}

		public void addMessageListener(MessageListener listener) {
			ListenerManager.get().addMapListener(name, listener, null, true,
					ListenerManager.LISTENER_TYPE_MESSAGE);
		}

		public void removeMessageListener(MessageListener listener) {
			ListenerManager.get().removeMapListener(name, listener, null);
		}

		@Override
		public String toString() {
			return "Topic [" + getName() + "]";
		}

		public String getName() {
			return name.substring(2);
		}
	}

	static class ListProxy extends CollectionProxy implements IList {
		public ListProxy(CProxy mapProxy) {
			super(mapProxy);
		}

		public void add(int index, Object element) {
			throw new UnsupportedOperationException();
		}

		public boolean addAll(int index, Collection c) {
			throw new UnsupportedOperationException();
		}

		public Object get(int index) {
			throw new UnsupportedOperationException();
		}

		public int indexOf(Object o) {
			throw new UnsupportedOperationException();
		}

		public int lastIndexOf(Object o) {
			throw new UnsupportedOperationException();
		}

		public ListIterator listIterator() {
			throw new UnsupportedOperationException();
		}

		public ListIterator listIterator(int index) {
			throw new UnsupportedOperationException();
		}

		public Object remove(int index) {
			throw new UnsupportedOperationException();
		}

		public Object set(int index, Object element) {
			throw new UnsupportedOperationException();
		}

		public List subList(int fromIndex, int toIndex) {
			throw new UnsupportedOperationException();
		}

		@Override
		public String toString() {
			return "List [" + getName() + "]";
		}
	}

	static class SetProxy extends CollectionProxy implements ISet {
		public SetProxy(CProxy mapProxy) {
			super(mapProxy);
		}

		@Override
		public String toString() {
			return "Set [" + getName() + "]";
		}
	}

	static class CollectionProxy extends AbstractCollection implements ICollection, IProxy {
		String name = null;

		CProxy mapProxy;

		public CollectionProxy(CProxy mapProxy) {
			super();
			this.mapProxy = mapProxy;
			this.name = mapProxy.name;
		}

		public void addItemListener(ItemListener listener, boolean includeValue) {
			mapProxy.addGenericListener(listener, null, includeValue,
					ListenerManager.LISTENER_TYPE_ITEM);
		}

		public void removeItemListener(ItemListener listener) {
			mapProxy.removeGenericListener(listener, null);
		}

		@Override
		public boolean add(Object obj) {
			return mapProxy.add(obj);
		}

		@Override
		public boolean remove(Object obj) {
			return mapProxy.removeKey(obj);
		}

		public boolean removeKey(Object obj) {
			return mapProxy.removeKey(obj);
		}

		@Override
		public boolean contains(Object obj) {
			return mapProxy.containsKey(obj);
		}

		@Override
		public Iterator iterator() {
			return mapProxy.values().iterator();
		}

		@Override
		public int size() {
			return mapProxy.size();
		}

		public String getName() {
			return name.substring(4);
		}

		public CProxy getCProxy() {
			return mapProxy;
		}
	}

	public static class BProxy extends AbstractQueue implements Constants, IQueue, BlockingQueue {

		String name = null;

		public BProxy(String qname) {
			this.name = qname;
		}

		public boolean offer(Object obj) {
			Offer offer = ThreadContext.get().getOffer();
			return offer.offer(name, obj, 0, ThreadContext.get().getTxnId());
		}

		public boolean offer(Object obj, long timeout, TimeUnit unit) throws InterruptedException {
			if (timeout < 0) {
				timeout = 0;
			}
			Offer offer = ThreadContext.get().getOffer();
			return offer.offer(name, obj, unit.toMillis(timeout), ThreadContext.get().getTxnId());
		}

		public void put(Object obj) throws InterruptedException {
			Offer offer = ThreadContext.get().getOffer();
			offer.offer(name, obj, -1, ThreadContext.get().getTxnId());
		}

		public Object peek() {
			Poll poll = BlockingQueueManager.get().new Poll();
			return poll.peek(name);
		}

		public Object poll() {
			Poll poll = BlockingQueueManager.get().new Poll();
			return poll.poll(name, 0);
		}

		public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
			if (timeout < 0) {
				timeout = 0;
			}
			Poll poll = BlockingQueueManager.get().new Poll();
			return poll.poll(name, unit.toMillis(timeout));
		}

		public Object take() throws InterruptedException {
			Poll poll = BlockingQueueManager.get().new Poll();
			return poll.poll(name, -1);
		}

		public int remainingCapacity() {
			return 0;
		}

		@Override
		public Iterator iterator() {
			QIterator iterator = BlockingQueueManager.get().new QIterator();
			iterator.set(name);
			return iterator;
		}

		@Override
		public int size() {
			Size size = BlockingQueueManager.get().new Size();
			return size.getSize(name);
		}

		public void addItemListener(ItemListener listener, boolean includeValue) {
			ListenerManager.get().addMapListener(name, listener, null, includeValue,
					ListenerManager.LISTENER_TYPE_ITEM);
		}

		public void removeItemListener(ItemListener listener) {
			ListenerManager.get().removeMapListener(name, listener, null);
		}

		public String getName() {
			return name;
		}

		public int drainTo(Collection c) {
			return 0;
		}

		public int drainTo(Collection c, int maxElements) {
			return 0;
		}

	}
	
	interface IProxy {
		boolean add(Object item);
		boolean removeKey (Object key); 
	}

	static class CProxy implements IMap, IProxy, Constants{

		String name = null;

		byte mapType = MAP_TYPE_MAP;

		public CProxy(String name) {
			super();
			this.name = name;
			this.mapType = BaseManager.getMapType(name);
		}

		@Override
		public String toString() {
			return "Map [" + getName() + "]";
		}

		public String getName() {
			return name.substring(2);
		}

		public Object put(Object key, Object value) {
			check(key);
			check(value);
			MPut mput = ThreadContext.get().getMPut();
			return mput.put(name, key, value, -1, -1);
		}

		public Object get(Object key) {
			check(key);
			MGet mget = ThreadContext.get().getMGet();
			return mget.get(name, key, -1, -1);
		}

		private void check(Object obj) {
			if (obj == null)
				throw new RuntimeException();
			if (obj instanceof DataSerializable)
				return;
			if (obj instanceof Serializable)
				return;
			else
				throw new IllegalArgumentException(obj.getClass().getName()
						+ " is not Serializable.");
		}

		public Object remove(Object key) {
			check(key);
			MRemove mremove = ThreadContext.get().getMRemove();
			return mremove.remove(name, key, -1, -1);
		}

		public int size() {
			MSize msize = ConcurrentMapManager.get().new MSize();
			return msize.getSize(name);
		}

		public Object putIfAbsent(Object key, Object value) {
			check(key);
			check(value);
			MPut mput = ThreadContext.get().getMPut();
			return mput.putIfAbsent(name, key, value, -1, -1);
		}

		public boolean remove(Object key, Object value) {
			check(key);
			check(value);
			MRemove mremove = ThreadContext.get().getMRemove();
			return (mremove.removeIfSame(name, key, value, -1, -1) != null);
		}

		public Object replace(Object key, Object value) {
			check(key);
			check(value);
			MPut mput = ThreadContext.get().getMPut();
			return mput.replace(name, key, value, -1, -1);
		}

		public boolean replace(Object key, Object oldValue, Object newValue) {
			check(key);
			check(newValue);
			throw new UnsupportedOperationException();
		}

		public void lock(Object key) {
			check(key);
			MLock mlock = ThreadContext.get().getMLock();
			mlock.lock(name, key, -1, -1);
		}

		public boolean tryLock(Object key) {
			check(key);
			MLock mlock = ThreadContext.get().getMLock();
			return mlock.lock(name, key, 0, -1);
		}

		public boolean tryLock(Object key, long time, TimeUnit timeunit) {
			check(key);
			if (time < 0)
				throw new IllegalArgumentException("Time cannot be negative. time = " + time);
			MLock mlock = ThreadContext.get().getMLock();
			return mlock.lock(name, key, timeunit.toMillis(time), -1);
		}

		public void unlock(Object key) {
			check(key);
			MLock mlock = ThreadContext.get().getMLock();
			mlock.unlock(name, key, 0, -1);
		}

		void addGenericListener(Object listener, Object key, boolean includeValue, int listenerType) {
			if (listener == null)
				throw new IllegalArgumentException("Listener cannot be null");
			ListenerManager.get().addMapListener(name, listener, key, includeValue, listenerType);
		}

		public void removeGenericListener(Object listener, Object key) {
			if (listener == null)
				throw new IllegalArgumentException("Listener cannot be null");
			ListenerManager.get().removeMapListener(name, listener, key);
		}

		public void addEntryListener(EntryListener listener, boolean includeValue) {
			if (listener == null)
				throw new IllegalArgumentException("Listener cannot be null");
			addGenericListener(listener, null, includeValue, ListenerManager.LISTENER_TYPE_MAP);
		}

		public void addEntryListener(EntryListener listener, Object key, boolean includeValue) {
			if (listener == null)
				throw new IllegalArgumentException("Listener cannot be null");
			check(key);
			addGenericListener(listener, key, includeValue, ListenerManager.LISTENER_TYPE_MAP);
		}

		public void removeEntryListener(EntryListener listener) {
			if (listener == null)
				throw new IllegalArgumentException("Listener cannot be null");
			removeGenericListener(listener, null);
		}

		public void removeEntryListener(EntryListener listener, Object key) {
			if (listener == null)
				throw new IllegalArgumentException("Listener cannot be null");
			check(key);
			removeGenericListener(listener, key);
		}

		public boolean containsKey(Object key) {
			check(key);
			MContainsKey mContainsKey = ConcurrentMapManager.get().new MContainsKey();
			return mContainsKey.containsKey(name, key, -1);
		}

		public boolean containsValue(Object value) {
			check(value);
			MContainsValue mContainsValue = ConcurrentMapManager.get().new MContainsValue();
			return mContainsValue.containsValue(name, value, -1);
		}

		public boolean isEmpty() {
			return (size() == 0);
		}

		public void putAll(Map map) {
			Set<Map.Entry> entries = map.entrySet();
			for (Entry entry : entries) {
				put(entry.getKey(), entry.getValue());
			}
		}

		public boolean add(Object value) {
			if (value == null)
				throw new NullPointerException();
			MAdd madd = ThreadContext.get().getMAdd();
			if (mapType == MAP_TYPE_LIST) {
				return madd.addToList(name, value);
			} else {
				return madd.addToSet(name, value);
			}
		}

		public boolean removeKey(Object key) {
			if (key == null)
				throw new NullPointerException();
			try {
				Data dataKey = ThreadContext.get().toData(key);
				return remove(dataKey) != null;
			} catch (Exception e) {
				e.printStackTrace();
			}
			return false;
		}

		public void clear() {
		}

		public Set entrySet() {
			return new EntrySet(EntrySet.TYPE_ENTRIES);
		}

		public Set keySet() {
			return new EntrySet(EntrySet.TYPE_KEYS);
		}

		public Collection values() {
			return new EntrySet(EntrySet.TYPE_VALUES);
		}

		class EntrySet extends AbstractSet {
			final static int TYPE_ENTRIES = 1;
			final static int TYPE_KEYS = 2;
			final static int TYPE_VALUES = 3;

			int type = TYPE_ENTRIES;

			public EntrySet(int type) {
				super();
				this.type = type;
			}

			@Override
			public Iterator iterator() {
				MIterator it = ConcurrentMapManager.get().new MIterator();
				it.set(name, type);
				return it;
			}

			@Override
			public int size() {
				return CProxy.this.size();
			}
		}

	}

}
