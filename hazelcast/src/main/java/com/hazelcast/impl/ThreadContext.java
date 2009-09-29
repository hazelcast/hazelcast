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

import com.hazelcast.collection.SimpleBoundedQueue;
import com.hazelcast.core.Transaction;
import com.hazelcast.impl.ConcurrentMapManager.MEvict;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.Serializer;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ThreadContext {

    private final static Logger logger = Logger.getLogger(ThreadContext.class.getName());

    private final static ThreadLocal<ThreadContext> threadLocal = new ThreadLocal<ThreadContext>();

    private final Serializer serializer = new Serializer();

    CallContext callContext = null;

    private final ObjectPool<Packet> packetCache;

    private final static ConcurrentMap<String, BlockingQueue> mapGlobalQueues = new ConcurrentHashMap<String, BlockingQueue>();

    private final ConcurrentMap<FactoryImpl, CallCache> mapCallCacheForFactories = new ConcurrentHashMap<FactoryImpl, CallCache>();

    private final static AtomicInteger newThreadId = new AtomicInteger();

    static {
        mapGlobalQueues.put("PacketCache", new ArrayBlockingQueue(2000));
    }

    private ThreadContext() {
        callContext = new CallContext(createNewThreadId(), false);
        int packetCacheSize = 0;
        String threadName = Thread.currentThread().getName();
        if (threadName.startsWith("hz.")) {
            if ("hz.InThread".equals(threadName)) {
                packetCacheSize = 100;
            } else if ("hz.OutThread".equals(threadName)) {
                packetCacheSize = 0;
            } else if ("hz.ServiceThread".equals(threadName)) {
                packetCacheSize = 100;
            }
        }
        packetCache = new ObjectPool<Packet>("PacketCache", packetCacheSize) {
            public Packet createNew() {
                return new Packet();
            }

            public void onRelease(Packet packet) {
                packet.reset();
                packet.released = true;
            }

            public void onObtain(Packet packet) {
                packet.reset();
                packet.released = false;
            }
        };
    }

    public static ThreadContext get() {
        ThreadContext threadContext = threadLocal.get();
        if (threadContext == null) {
            threadContext = new ThreadContext();
            threadLocal.set(threadContext);
        }
        return threadContext;
    }

    public ObjectPool<Packet> getPacketPool() {
        return packetCache;
    }

    public void finalizeTxn() {
        callContext.finalizeTxn();
    }

    public Transaction getTransaction() {
        return callContext.getTxn();
    }

    public long getTxnId() {
        return callContext.getTxnId();
    }

    public CallContext getExecutionContext() {
        return callContext;
    }

    public void setExecutionContext(CallContext executionContext) {
        this.callContext = executionContext;
    }

    public void reset() {
        finalizeTxn();
    }

    public Data toData(final Object obj) {
        if (obj == null) return null;
        if (obj instanceof Data) return (Data) obj;
        try {
            return serializer.writeObject(obj);
        } catch (final Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public Object toObject(final Data data) {
        return serializer.readObject(data);
    }

    public CallCache getCallCache(FactoryImpl factory) {
        CallCache callCache = mapCallCacheForFactories.get(factory);
        if (callCache == null) {
            callCache = new CallCache(factory);
            mapCallCacheForFactories.put(factory, callCache);
        }
        return callCache;
    }

    /**
     * Is this thread remote Java or CSharp Client thread?
     *
     * @return true if the thread is for Java or CSharp Client, false otherwise
     */
    public boolean isClient() {
        return callContext.isClient();
    }

    public int createNewThreadId() {
        return newThreadId.incrementAndGet();
    }

    class CallCache {
        final FactoryImpl factory;
        final ConcurrentMapManager.MPut mput;
        final ConcurrentMapManager.MGet mget;
        final ConcurrentMapManager.MRemove mremove;
        final ConcurrentMapManager.MEvict mevict;

        CallCache(FactoryImpl factory) {
            this.factory = factory;
            mput = factory.node.concurrentMapManager.new MPut();
            mget = factory.node.concurrentMapManager.new MGet();
            mremove = factory.node.concurrentMapManager.new MRemove();
            mevict = factory.node.concurrentMapManager.new MEvict();
        }

        public ConcurrentMapManager.MPut getMPut() {
            mput.reset();
            return mput;
        }

        public ConcurrentMapManager.MGet getMGet() {
            mget.reset();
            return mget;
        }

        public ConcurrentMapManager.MRemove getMRemove() {
            mremove.reset();
            return mremove;
        }

        public MEvict getMEvict() {
            mevict.reset();
            return mevict;
        }
    }

    public abstract class ObjectPool<E> {
        private final String name;
        private final int maxSize;
        private final Queue<E> localPool;
        private final BlockingQueue<E> objectQueue;
        private int zero = 0;

        public ObjectPool(String name, int maxSize) {
            super();
            this.name = name;
            this.maxSize = maxSize;
            this.objectQueue = mapGlobalQueues.get(name);
            if (maxSize > 0) {
                this.localPool = new SimpleBoundedQueue<E>(maxSize);
            } else {
                this.localPool = null;
            }
        }

        public abstract E createNew();

        public abstract void onRelease(E e);

        public abstract void onObtain(E e);

        public String getName() {
            return name;
        }

        public void release(E obj) {
            onRelease(obj);
            if (localPool == null) {
                objectQueue.offer(obj);
            } else if (!localPool.add(obj)) {
                objectQueue.offer(obj);
            }
        }

        public E obtain() {
            E value;
            if (localPool == null) {
                value = objectQueue.poll();
                if (value == null) {
                    value = createNew();
                }
            } else {
                value = localPool.poll();
                if (value == null) {
                    int totalDrained = objectQueue.drainTo(localPool, maxSize);
                    if (totalDrained == 0) {
                        if (++zero % 10000 == 0) {
                            logger.log(Level.FINEST, "ObjectPool [" + name + "] : "
                                    + Thread.currentThread().getName()
                                    + " DRAINED " + totalDrained + "  size:" + objectQueue.size()
                                    + ", zeroCount:" + zero);
                            zero = 0;
                        }
                        for (int i = 0; i < 4; i++) {
                            localPool.add(createNew());
                        }
                    }
                    value = localPool.poll();
                    if (value == null) {
                        value = createNew();
                    }
                }
            }
            onObtain(value);
            return value;
        }
    }

    public int getThreadId() {
        return callContext.getThreadId();
    }

    public void setCallContext(CallContext callContext) {
        this.callContext = callContext;
    }
}
