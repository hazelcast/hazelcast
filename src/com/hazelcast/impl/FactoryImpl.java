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

import com.hazelcast.core.*;
import com.hazelcast.impl.BaseManager.Processable;
import com.hazelcast.impl.BlockingQueueManager.*;
import com.hazelcast.impl.ClusterManager.CreateProxy;
import com.hazelcast.impl.ConcurrentMapManager.*;
import static com.hazelcast.impl.Constants.MapTypes.*;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;

public class FactoryImpl implements Constants {

    private static ConcurrentMap<String, ICommon> proxies = new ConcurrentHashMap<String, ICommon>(1000);

    private static MProxy locksMapProxy = new MProxy("m:__hz_Locks");

    private static MProxy idGeneratorMapProxy = new MProxy("m:__hz_IdGenerator");

    private static ConcurrentMap<Object, LockProxy> mapLockProxies = new ConcurrentHashMap<Object, LockProxy>(100);

    private static ConcurrentMap<String, IdGenerator> mapIdGenerators = new ConcurrentHashMap<String, IdGenerator>(100);

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

    public static void shutdown() {
        Node.get().shutdown();
    }

    public static Collection<ICommon> getInstances() {
        List<ICommon> lsProxies = new ArrayList<ICommon>(proxies.size() + mapLockProxies.size());
        lsProxies.addAll(proxies.values());
        lsProxies.addAll(mapLockProxies.values());
        lsProxies.addAll(mapIdGenerators.values());
        return lsProxies;
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
        if (!inited.get()) {
            init();
        }
        IdGenerator idGenerator = mapIdGenerators.get(name);
        if (idGenerator != null)
            return idGenerator;
        synchronized (IdGeneratorProxy.class) {
            idGenerator = new IdGeneratorProxy(name);
            IdGeneratorProxy old = (IdGeneratorProxy) mapIdGenerators.putIfAbsent(name, idGenerator);
            if (old != null)
                idGenerator = old;
        }
        return idGenerator;
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

    public static <K, V> MultiMap<K, V> getMultiMap(String name) {
        name = "m:u:" + name;
        return (MultiMap<K, V>) getProxy(name);
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

    public static ILock getLock(Object key) {
        if (!inited.get())
            init();
        LockProxy lockProxy = mapLockProxies.get(key);
        if (lockProxy == null) {
            lockProxy = new LockProxy(locksMapProxy, key);
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
        ICommon proxy = proxies.get(name);
        if (proxy == null) {
            if (name.startsWith("q:")) {
                proxy = proxies.get(name);
                if (proxy == null) {
                    proxy = new QProxy(name);
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
                    proxy = new MProxy(name);
                    proxies.put(name, proxy);
                }
            } else if (name.startsWith("m:")) {
                proxy = proxies.get(name);
                if (proxy == null) {
                    byte mapType = BaseManager.getMapType(name);
                    MProxy mapProxy = new MProxy(name);
                    if (mapType == MAP_TYPE_SET) {
                        proxy = new SetProxy(mapProxy);
                    } else if (mapType == MAP_TYPE_LIST) {
                        proxy = new ListProxy(mapProxy);
                    } else if (mapType == MAP_TYPE_MULTI_MAP) {
                        proxy = new MultiMapProxy(mapProxy);
                    } else {
                        proxy = mapProxy;
                    }
                    proxies.put(name, proxy);
                }
            }
        }
        return proxy;
    }

    static class LockProxy implements ILock, ICommon {

        MProxy mapProxy = null;

        Object key = null;

        public LockProxy(MProxy mapProxy, Object key) {
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

        public void destroy() {
            mapProxy.remove(key);
            mapLockProxies.remove(key);
        }

        public ICommon.InstanceType getInstanceType() {
            return ICommon.InstanceType.LOCK;
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

    static class TopicProxy implements ITopic, ICommon {
        String name;
        QProxy qProxy = null; // for global ordering support

        public TopicProxy(String name) {
            super();
            this.name = name;
            // if (Config.get().getTopicConfig(getName()).globalOrderingEnabled)
            // {
            // qProxy = new QProxy("q:" + name);
            // }
        }

        public void publish(Object msg) {
            if (qProxy == null) {
                TopicManager.get().doPublish(name, msg);
            } else {
                qProxy.publish(msg);
            }
        }

        public void addMessageListener(MessageListener listener) {
            if (qProxy == null) {
                ListenerManager.get().addListener(name, listener, null, true,
                        ListenerManager.LISTENER_TYPE_MESSAGE);
            } else {
                AddTopicListener atl = BlockingQueueManager.get().new AddTopicListener();
                atl.add(qProxy.name, null, 0, -1);
                ListenerManager.get().addListener(qProxy.name, listener, null, true,
                        ListenerManager.LISTENER_TYPE_MESSAGE, false);
            }
        }

        public void removeMessageListener(MessageListener listener) {
            if (qProxy == null) {
                ListenerManager.get().removeListener(name, listener, null);
            } else {
                ListenerManager.get().removeListener(qProxy.name, listener, null);
            }
        }

        public void destroy() {
            TopicManager.TopicDestroy topicDestroy = TopicManager.get().new TopicDestroy();
            topicDestroy.destroy(name);
            proxies.remove(name);
        }

        public ICommon.InstanceType getInstanceType() {
            return ICommon.InstanceType.TOPIC;
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
        public ListProxy(MProxy mapProxy) {
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

        public ICommon.InstanceType getInstanceType() {
            return ICommon.InstanceType.LIST;
        }

        @Override
        public String toString() {
            return "List [" + getName() + "]";
        }
    }

    static class SetProxy extends CollectionProxy implements ISet {
        public SetProxy(MProxy mapProxy) {
            super(mapProxy);
        }

        @Override
        public String toString() {
            return "Set [" + getName() + "]";
        }

        public ICommon.InstanceType getInstanceType() {
            return ICommon.InstanceType.SET;
        }
    }

    static abstract class CollectionProxy extends AbstractCollection implements ICollection, IRemoveAwareProxy, ICommon {
        String name = null;

        MProxy mapProxy;

        public CollectionProxy(MProxy mapProxy) {
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
            return mapProxy.keySet().iterator();
        }

        @Override
        public int size() {
            return mapProxy.size();
        }

        public String getName() {
            return name.substring(4);
        }

        public MProxy getCProxy() {
            return mapProxy;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o);
        }

        public void destroy() {
            mapProxy.destroy();
            proxies.remove(name);
        }
    }

    public static class QProxy extends AbstractQueue implements Constants, IQueue, BlockingQueue, ICommon {

        String name = null;

        public QProxy(String qname) {
            this.name = qname;
        }

        public boolean publish(Object obj) {
            Offer offer = ThreadContext.get().getOffer();
            return offer.publish(name, obj, 0, ThreadContext.get().getTxnId());
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
            throw new UnsupportedOperationException();
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
            ListenerManager.get().addListener(name, listener, null, includeValue,
                    ListenerManager.LISTENER_TYPE_ITEM);
        }

        public void removeItemListener(ItemListener listener) {
            ListenerManager.get().removeListener(name, listener, null);
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean remove(Object obj) {
            throw new UnsupportedOperationException();
        }

        public int drainTo(Collection c) {
            throw new UnsupportedOperationException();
        }

        public int drainTo(Collection c, int maxElements) {
            throw new UnsupportedOperationException();
        }

        public void destroy() {
            clear();
            QDestroy qDestroy = BlockingQueueManager.get().new QDestroy();
            qDestroy.destroy(name);
            proxies.remove(name);
        }

        public ICommon.InstanceType getInstanceType() {
            return ICommon.InstanceType.QUEUE;
        }
    }

    static class MultiMapProxy implements MultiMap, Constants, ICommon {
        String name = null;

        MProxy mapProxy;

        public MultiMapProxy(MProxy mapProxy) {
            super();
            this.mapProxy = mapProxy;
            this.name = mapProxy.name;
        }

        public String getName() {
            return name.substring(4);
        }

        public void clear() {
            mapProxy.clear();
        }

        public boolean containsEntry(Object key, Object value) {
            return mapProxy.containsEntry(key, value);
        }

        public boolean containsKey(Object key) {
            return mapProxy.containsKey(key);
        }

        public boolean containsValue(Object value) {
            return mapProxy.containsValue(value);
        }

        public Collection get(Object key) {
            return (Collection) mapProxy.get(key);
        }


        public boolean put(Object key, Object value) {
            return mapProxy.putMulti(key, value);
        }

        public boolean remove(Object key, Object value) {
            return mapProxy.removeMulti(key, value);
        }

        public boolean remove(Object key) {
            return false;
        }

        public Collection removeAll(Object key) {
            return null;
        }

        public int size() {
            return mapProxy.size();
        }

        public int valueCount(Object key) {
            return 0;
        }

        public Set keySet() {
            return mapProxy.keySet();
        }

        public Collection values() {
            return mapProxy.values();
        }

        public Set entrySet() {
            return mapProxy.entrySet();
        }

        public ICommon.InstanceType getInstanceType() {
            return ICommon.InstanceType.MULTIMAP;
        }

        public void destroy() {
            mapProxy.destroy();
            proxies.remove(name);
        }
    }

    interface IRemoveAwareProxy {

        boolean removeKey(Object key);
    }

    private static void check(Object obj) {
        if (obj == null)
            throw new RuntimeException("Object cannot be null.");

        if (!(obj instanceof Serializable)) {
            throw new IllegalArgumentException(obj.getClass().getName() + " is not Serializable.");
        }
    }

    static class MProxy implements IMap, IRemoveAwareProxy, ICommon, Constants {

        String name = null;

        byte mapType = MAP_TYPE_MAP;

        public MProxy(String name) {
            super();
            this.name = name;
            this.mapType = BaseManager.getMapType(name);
        }

        public ICommon.InstanceType getInstanceType() {
            if (mapType == MAP_TYPE_MAP)
                return ICommon.InstanceType.MAP;
            else if (mapType == MAP_TYPE_SET)
                return ICommon.InstanceType.SET;
            else if (mapType == MAP_TYPE_LIST)
                return ICommon.InstanceType.LIST;
            else throw new RuntimeException("Unknown MProxy type " + mapType);
        }

        @Override
        public String toString() {
            return "Map [" + getName() + "]";
        }

        public String getName() {
            return name.substring(2);
        }

        public boolean putMulti(Object key, Object value) {
            check(key);
            check(value);
            MPutMulti mput = ThreadContext.get().getMPutMulti();
            return mput.put(name, key, value);
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

        public Object remove(Object key) {
            check(key);
            MRemove mremove = ThreadContext.get().getMRemove();
            return mremove.remove(name, key, -1, -1);
        }

        public int size() {
//            MSize msize = ConcurrentMapManager.get().new MSize();
//            int size = msize.getSize(name);
            MSize msize = ConcurrentMapManager.get().new MSize(name);
            int size = (Integer) msize.call();
            TransactionImpl txn = ThreadContext.get().txn;
            if (txn != null) {
                size += txn.size(name);
            }
            return (size < 0) ? 0 : size;
        }

        public Object putIfAbsent(Object key, Object value) {
            check(key);
            check(value);
            MPut mput = ThreadContext.get().getMPut();
            return mput.putIfAbsent(name, key, value, -1, -1);
        }

        public boolean removeMulti(Object key, Object value) {
            check(key);
            check(value);
            MRemoveMulti mremove = ThreadContext.get().getMRemoveMulti();
            return mremove.remove(name, key, value);
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
            ListenerManager.get().addListener(name, listener, key, includeValue, listenerType);
        }

        public void removeGenericListener(Object listener, Object key) {
            if (listener == null)
                throw new IllegalArgumentException("Listener cannot be null");
            ListenerManager.get().removeListener(name, listener, key);
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

        public boolean containsEntry(Object key, Object value) {
            check(key);
            check(value);
            TransactionImpl txn = ThreadContext.get().txn;
            if (txn != null) {
                if (txn.has(name, key)) {
                    Object v = txn.get(name, key);
                    if (v == null)
                        return false; // removed inside the txn
                    else
                        return true;
                }
            }
            MContainsKey mContainsKey = ConcurrentMapManager.get().new MContainsKey();
            return mContainsKey.containsEntry(name, key, value, -1);
        }

        public boolean containsKey(Object key) {
            check(key);
            TransactionImpl txn = ThreadContext.get().txn;
            if (txn != null) {
                if (txn.has(name, key)) {
                    Object value = txn.get(name, key);
                    if (value == null)
                        return false; // removed inside the txn
                    else
                        return true;
                }
            }
            MContainsKey mContainsKey = ConcurrentMapManager.get().new MContainsKey();
            return mContainsKey.containsKey(name, key, -1);
        }

        public boolean containsValue(Object value) {
            check(value);
            TransactionImpl txn = ThreadContext.get().txn;
            if (txn != null) {
                if (txn.containsValue(name, value))
                    return true;
            }
            MContainsValue mContainsValue = ConcurrentMapManager.get().new MContainsValue(name, value);
            return (Boolean) mContainsValue.call();
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
            MRemoveItem mRemoveItem = ConcurrentMapManager.get().new MRemoveItem();
            return mRemoveItem.removeItem(name, key);
        }

        public void clear() {
            Set keys = keySet();
            for (Object key : keys) {
                removeKey(key);
            }
        }

        public Set entrySet() {
            return (Set) iterate(MIterate.TYPE_ENTRIES);
        }

        public Set keySet() {
            return (Set) iterate(MIterate.TYPE_KEYS);
        }

        public Collection values() {
            return iterate(MIterate.TYPE_VALUES);

        }

        private Collection iterate(int iteratorType) {
            MIterate miterate = ConcurrentMapManager.get().new MIterate(name, iteratorType);
            return (Collection) miterate.call();
        }

        public void destroy() {
            clear();
            MDestroy mDestroy = ConcurrentMapManager.get().new MDestroy();
            mDestroy.destroy(name);
            proxies.remove(name);
        }

    }

    static class IdGeneratorProxy implements IdGenerator {

        private static final long BILLION = 1 * 1000 * 1000;

        private final String name;

        public IdGeneratorProxy(String name) {
            this.name = name;
        }

        AtomicLong billion = new AtomicLong(-1);

        AtomicLong currentId = new AtomicLong(2 * BILLION);

        AtomicBoolean fetching = new AtomicBoolean(false);

        public String getName() {
            return name;
        }

        public long newId() {
            long billionNow = billion.get();
            long idAddition = currentId.incrementAndGet();
            if (idAddition >= BILLION) {
                synchronized (this) {
                    try {
                        billionNow = billion.get();
                        idAddition = currentId.incrementAndGet();
                        if (idAddition >= BILLION) {
                            Long idBillion = getNewBillion();
                            long newBillion = idBillion.longValue() * BILLION;
                            billion.set(newBillion);
                            currentId.set(0);
                        }
                        billionNow = billion.get();
                        idAddition = currentId.incrementAndGet();
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }

            }
            long result = billionNow + idAddition;
            return result;
        }

        private Long getNewBillion() {
            try {
                DistributedTask<Long> task = new DistributedTask<Long>(new IncrementTask(name));
                FactoryImpl.executorServiceImpl.execute(task);
                return task.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }


        public InstanceType getInstanceType() {
            return ICommon.InstanceType.ID_GENERATOR;
        }

        public void destroy() {
            mapIdGenerators.remove(name);
            idGeneratorMapProxy.remove(name);
        }
    }

    public static class IncrementTask implements Callable<Long>, Serializable {
        String name = null;

        public IncrementTask() {
            super();
        }

        public IncrementTask(String uuidName) {
            super();
            this.name = uuidName;
        }

        public Long call() {
            MProxy map = FactoryImpl.idGeneratorMapProxy;
            map.lock(name);
            try {
                Long max = (Long) map.get(name);
                if (max == null) {
                    max = Long.valueOf(0l);
                    map.put(name, Long.valueOf(0));
                    return max;
                } else {
                    Long newMax = Long.valueOf(max.longValue() + 1);
                    map.put(name, newMax);
                    return newMax;
                }
            } finally {
                map.unlock(name);
            }
        }
    }

}
