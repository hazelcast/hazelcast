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
import com.hazelcast.impl.ConcurrentMapManager.*;
import com.hazelcast.nio.BufferUtil;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.Serializer;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import static com.hazelcast.impl.Constants.IO.BYTE_BUFFER_SIZE;


public final class ThreadContext {

    private final static Logger logger = Logger.getLogger(ThreadContext.class.getName());

    private final static ThreadLocal<ThreadContext> threadLocal = new ThreadLocal<ThreadContext>();

    private final Serializer serializer = new Serializer();

    long txnId = -1;

    TransactionImpl txn = null;

    final ObjectPool<ByteBuffer> bufferCache;

    final ObjectPool<Packet> packetCache;

    final static ConcurrentMap<String, BlockingQueue> mapGlobalQueues = new ConcurrentHashMap();

    static {
        mapGlobalQueues.put("BufferCache", new ArrayBlockingQueue(6000));
        mapGlobalQueues.put("PacketCache", new ArrayBlockingQueue(2000));
    }

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

        bufferCache = new ObjectPool<ByteBuffer>("BufferCache", bufferCacheSize) {
            public ByteBuffer createNew() {
                return ByteBuffer.allocate(BYTE_BUFFER_SIZE);
            }

            public void onRelease(ByteBuffer byteBuffer) {
                byteBuffer.clear();
            }

            public void onObtain(ByteBuffer byteBuffer) {
                byteBuffer.clear();
            }
        };

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

    MGet mget = ConcurrentMapManager.get().new MGet();

    public MGet getMGet() {
        mget.reset();
        return mget;
    }

    public MLock getMLock() {
        return ConcurrentMapManager.get().new MLock();
    }

    MPut mput = ConcurrentMapManager.get().new MPut();

    public MPut getMPut() {
        mput.reset();
        return mput;
    }

    public MPutMulti getMPutMulti() {
        return ConcurrentMapManager.get().new MPutMulti();
    }

    MRemove mremove = ConcurrentMapManager.get().new MRemove();

    public MRemove getMRemove() {
        mremove.reset();
        return mremove;
    }

    public MRemoveMulti getMRemoveMulti() {
        return ConcurrentMapManager.get().new MRemoveMulti();
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

    public abstract class ObjectPool<E> {
        private final String name;
        private final int maxSize;
        private final Queue<E> localPool;
        private final BlockingQueue<E> objectQueue;

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
//						if (++zero % 10000 == 0) {
//							System.out.println(name + " : " + Thread.currentThread().getName()
//									+ " DRAINED " + totalDrained + "  size:" + objectQueue.size()
//									+ ", zeroCount:" + zero);
//						}		
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

}
