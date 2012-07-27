/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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
 */

package com.hazelcast.impl;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.serialization.SerializationHelper;
import com.hazelcast.nio.serialization.SerializerRegistry;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public final class ThreadContext {

    private static final AtomicInteger newThreadId = new AtomicInteger();

    private static final ConcurrentMap<Thread, ThreadContext> mapContexts = new ConcurrentHashMap<Thread, ThreadContext>(1000);

    public static ThreadContext get() {
        Thread currentThread = Thread.currentThread();
        ThreadContext threadContext = mapContexts.get(currentThread);
        if (threadContext == null) {
            threadContext = new ThreadContext(Thread.currentThread());
            mapContexts.put(currentThread, threadContext);
            Iterator<Entry<Thread,ThreadContext>> threads = mapContexts.entrySet().iterator();
            while (threads.hasNext()) {
                Entry<Thread, ThreadContext> entry = threads.next();
                if (!entry.getKey().isAlive()) {
                    entry.getValue().destroy();
                    threads.remove();
                }
            }
            if (mapContexts.size() > 1000) {
                String msg = " ThreadContext is created!! You might have too many threads. Is that normal?";
                Logger.getLogger(ThreadContext.class.getName()).log(Level.WARNING, mapContexts.size() + msg);
            }
        }
        return threadContext;
    }

    public static int createNewThreadId() {
        return newThreadId.incrementAndGet();
    }

    public static void shutdownAll() {
        mapContexts.clear();
    }

    public static synchronized void shutdown(Thread thread) {
        ThreadContext threadContext = mapContexts.remove(thread);
        if (threadContext != null) {
            threadContext.destroy();
        }
    }

    private final Thread thread;

    private final SerializationHelper serializationHelper = new SerializationHelper(this);

    private final Map<String, HazelcastInstanceThreadContext> mapHazelcastInstanceContexts
            = new HashMap<String, HazelcastInstanceThreadContext>(2);

    private FactoryImpl currentFactory ;

    private SerializerRegistry currentSerializerRegistry;

    private Object attachment;

    private ThreadContext(Thread thread) {
        this.thread = thread;
    }

    public void finalizeTxn() {
        getCallContext().finalizeTransaction();
    }

    public TransactionImpl getTransaction() {
        return getCallContext().getTransaction();
    }

    public long getTxnId() {
        return getCallContext().getTxnId();
    }

    public FactoryImpl getCurrentFactory() {
        return currentFactory;
    }

    public ManagedContext getCurrentManagedContext() {
        return currentFactory != null ? currentFactory.managedContext : null;
    }

    public SerializerRegistry getCurrentSerializerRegistry() {
        return currentSerializerRegistry != null ? currentSerializerRegistry : null ;
    }

    public void setCurrentFactory(FactoryImpl currentFactory) {
        this.currentFactory = currentFactory;
        this.currentSerializerRegistry = currentFactory.serializerRegistry;
    }

    public void setCurrentSerializerRegistry(SerializerRegistry serializerRegistry) {
        this.currentSerializerRegistry = serializerRegistry;
    }

    public void reset() {
        finalizeTxn();
    }

    public byte[] toByteArray(Object obj) {
        return serializationHelper.toByteArray(obj);
    }

    public Data toData(Object obj) {
        return serializationHelper.writeObject(obj);
    }

    public Object toObject(Data data) {
        return serializationHelper.readObject(data);
    }

    public Object toObject(byte[] data) {
        return serializationHelper.toObject(data);
    }

    public HazelcastInstanceThreadContext getHazelcastInstanceThreadContext(FactoryImpl factory) {
        if (factory == null) {
            ILogger logger = Logger.getLogger(ThreadContext.class.getName());
            logger.log(Level.SEVERE, "Factory is null", new Throwable());
        }
        final String factoryKey = factory != null ? factory.getName() : "null";
        HazelcastInstanceThreadContext hic = mapHazelcastInstanceContexts.get(factoryKey);
        if (hic != null) return hic;
        hic = new HazelcastInstanceThreadContext(factory);
        mapHazelcastInstanceContexts.put(factoryKey, hic);
        return hic;
    }

    public CallCache getCallCache(FactoryImpl factory) {
        return getHazelcastInstanceThreadContext(factory).getCallCache();
    }

    /**
     * Is this thread remote Java or CSharp Client thread?
     *
     * @return true if the thread is for Java or CSharp Client, false otherwise
     */
    public boolean isClient() {
        return getCallContext().isClient();
    }

    public void setCallContext(CallContext callContext) {
        getHazelcastInstanceThreadContext(currentFactory).setCallContext(callContext);
    }

    public CallContext getCallContext() {
        return getHazelcastInstanceThreadContext(currentFactory).getCallContext();
    }

    public int getThreadId() {
        return getCallContext().getThreadId();
    }

    public <T> T getAttachment() {
        return (T) attachment;
    }

    public void setAttachment(final Object attachment) {
        this.attachment = attachment;
    }

    public void shutdown(FactoryImpl factory) {
        mapHazelcastInstanceContexts.remove(factory.getName());
        currentFactory = null;
    }

    private void destroy() {
        serializationHelper.destroy();
        mapHazelcastInstanceContexts.clear();
        attachment = null;
        currentFactory = null;
        currentSerializerRegistry = null;
    }

    private class HazelcastInstanceThreadContext {
        final FactoryImpl factory;
        CallCache callCache;
        volatile CallContext callContext = null;

        HazelcastInstanceThreadContext(FactoryImpl factory) {
            this.factory = factory;
            callContext = (new CallContext(createNewThreadId(), false));
        }

        CallCache getCallCache() {
            if (callCache == null) {
                callCache = new CallCache(factory);
            }
            return callCache;
        }

        CallContext getCallContext() {
            return callContext;
        }

        void setCallContext(CallContext callContext) {
            this.callContext = callContext;
        }
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
            mput.request.lastTime = System.nanoTime();
            return mput;
        }

        public ConcurrentMapManager.MGet getMGet() {
            mget.reset();
            mget.request.lastTime = System.nanoTime();
            return mget;
        }

        public ConcurrentMapManager.MRemove getMRemove() {
            mremove.reset();
            mremove.request.lastTime = System.nanoTime();
            return mremove;
        }

        public ConcurrentMapManager.MEvict getMEvict() {
            mevict.reset();
            return mevict;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ThreadContext that = (ThreadContext) o;
        if (thread != null ? !thread.equals(that.thread) : that.thread != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return thread != null ? thread.hashCode() : 0;
    }
}
