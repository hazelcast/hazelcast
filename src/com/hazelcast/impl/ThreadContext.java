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

import java.nio.ByteBuffer;
import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hazelcast.core.Transaction;
import com.hazelcast.impl.BlockingQueueManager.Offer;
import com.hazelcast.impl.BlockingQueueManager.Poll;
import com.hazelcast.impl.ConcurrentMapManager.MAdd;
import com.hazelcast.impl.ConcurrentMapManager.MGet;
import com.hazelcast.impl.ConcurrentMapManager.MLock;
import com.hazelcast.impl.ConcurrentMapManager.MPut;
import com.hazelcast.impl.ConcurrentMapManager.MRemove;
import com.hazelcast.nio.BufferUtil;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.PacketQueue;
import com.hazelcast.nio.Serializer;
import com.hazelcast.nio.PacketQueue.Packet;

public class ThreadContext {

    private final static BlockingQueue<ByteBuffer> bufferq = new ArrayBlockingQueue<ByteBuffer>(6000);

    private final static Logger logger = Logger.getLogger(ThreadContext.class.getName());

    private final static ThreadLocal<ThreadContext> threadLocal = new ThreadLocal<ThreadContext>();

    private final Serializer serializer = new Serializer();

    long txnId = -1;

    TransactionImpl txn = null;

    final ObjectPool<ByteBuffer> bufferCache;

    final ObjectPool<PacketQueue.Packet> packetCache;

    private ThreadContext() {
        int bufferCacheSize = 12;
        int packetCacheSize = 0;
        String threadName = Thread.currentThread().getName();
        if (threadName.startsWith("hz.")) {
            if ("hz.InThread".equals(threadName)) {
                bufferCacheSize = 100;
                packetCacheSize = 100;
            } else if ("hz.OutThread".equals(threadName)) {
                bufferCacheSize = 0;
                packetCacheSize = 0;
            } else if ("hz.ServiceThread".equals(threadName)) {
                bufferCacheSize = 100;
                packetCacheSize = 100;
            }
        }
        logger.log(Level.FINEST, threadName + " is starting with cacheSize " + bufferCacheSize);

        ObjectFactory<ByteBuffer> byteBufferCacheFactory = new ObjectFactory<ByteBuffer>() {
            public ByteBuffer createNew() {
                return ByteBuffer.allocate(1024);
            }
        };
        bufferCache = new ObjectPool<ByteBuffer>("BufferCache", byteBufferCacheFactory,
                bufferCacheSize, bufferq);

        ObjectFactory<Packet> packetCacheFactory = new ObjectFactory<Packet>() {
            public Packet createNew() {
                return PacketQueue.get().createNewPacket();
            }
        };
        packetCache = new ObjectPool<Packet>("PacketCache", packetCacheFactory,
                packetCacheSize, PacketQueue.get().qPackets);
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

    public ObjectPool<ByteBuffer> getBufferPool() {
        return bufferCache;
    }

    public void finalizeTxn() {
        txn = null;
        txnId = -1;
    }

    public MAdd getMAdd() {
        return ConcurrentMapManager.get().new MAdd();
    }

    public MGet getMGet() {
        return ConcurrentMapManager.get().new MGet();
    }

    public MLock getMLock() {
        return ConcurrentMapManager.get().new MLock();
    }

    public MPut getMPut() {
        return ConcurrentMapManager.get().new MPut();
    }

    public MRemove getMRemove() {
        return ConcurrentMapManager.get().new MRemove();
    }

    public Offer getOffer() {
        return BlockingQueueManager.get().new Offer();
    }

    public Poll getPoll() {
        return BlockingQueueManager.get().new Poll();
    }

    public Transaction getTransaction() {
        if (txn == null) {
            txn = TransactionFactory.get().newTransaction();
            txnId = txn.getId();
        }
        return txn;
    }

    public long getTxnId() {
        return txnId;
    }

    public Data hardCopy(final Data data) {
        return BufferUtil.doHardCopy(data);
    }

    public void reset() {
        finalizeTxn();
    }

    public Data toData(final Object obj) {
        try {
            return serializer.writeObject(obj);
        } catch (final Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        return null;
    }

    public Object toObject(final Data data) {
        return serializer.readObject(data);
    }

    private interface ObjectFactory<E> {
        E createNew();
    }

    public class ObjectPool<E> {
        private final String name;
        private final int maxSize;
        private final Queue<E> localPool;
        private final ObjectFactory<E> objectFactory;
        private final BlockingQueue<E> objectQueue;
        private long zero = 0;

        public ObjectPool(String name, ObjectFactory<E> objectFactory, int maxSize,
                          BlockingQueue<E> objectQueue) {
            super();
            this.name = name;
            this.objectFactory = objectFactory;
            this.maxSize = maxSize;
            this.objectQueue = objectQueue;
            if (maxSize > 0) {
                this.localPool = new SimpleQueue<E>(maxSize);
            } else {
                this.localPool = null;
            }
        }

        public void release(E obj) {
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
                    value = objectFactory.createNew();
                }
            } else {
                value = localPool.poll();
                if (value == null) {
                    int totalDrained = objectQueue.drainTo(localPool, maxSize);
                    if (totalDrained == 0) {
//						if (++zero % 10000 == 0) {
//							System.out.println(name + " : " + Thread.currentThread().getName()
//									+ " DRAINED " + totalDrained + "  size:" + objectQueue.size()
//									+ ", zeroCount:" + zero);
//						}		
                        for (int i = 0; i < 4; i++) {
                            localPool.add(objectFactory.createNew());
                        }
                    }
                    value = localPool.poll();
                    if (value == null) {
                        value = objectFactory.createNew();
                    }
                }
            }

            return value;
        }
    }

    class SimpleQueue<E> extends AbstractQueue<E> {
        final int maxSize;
        final E[] objects;
        int add = 0;
        int remove = 0;
        int size = 0;

        public SimpleQueue(int maxSize) {
            this.maxSize = maxSize;
            objects = (E[]) new Object[maxSize];
        }

        @Override
        public boolean add(E obj) {
            if (size == maxSize)
                return false;
            objects[add] = obj;
            add++;
            size++;
            if (add == maxSize) {
                add = 0;
            }
            return true;
        }

        @Override
        public int size() {
            return size;
        }

        public boolean offer(E o) {
            return add(o);
        }

        public E peek() {
            return null;
        }

        public E poll() {
            if (size == 0)
                return null;
            E value = objects[remove];
            objects[remove] = null;
            remove++;
            size--;
            if (remove == maxSize) {
                remove = 0;
            }
            return value;
        }

        @Override
        public Iterator<E> iterator() {
            return null;
        }
    }
}
