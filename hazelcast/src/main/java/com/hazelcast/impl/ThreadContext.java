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

import com.hazelcast.core.Transaction;
import com.hazelcast.impl.ConcurrentMapManager.MEvict;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Serializer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class ThreadContext {

    private final static ThreadLocal<ThreadContext> threadLocal = new ThreadLocal<ThreadContext>();

    private final Serializer serializer = new Serializer();

    private CallContext callContext = null;

    private FactoryImpl currentFactory = null;

    private final ConcurrentMap<FactoryImpl, CallCache> mapCallCacheForFactories = new ConcurrentHashMap<FactoryImpl, CallCache>();

    private final static AtomicInteger newThreadId = new AtomicInteger();

    private ThreadContext() {
        setCallContext(new CallContext(createNewThreadId(), false));
    }

    public static ThreadContext get() {
        ThreadContext threadContext = threadLocal.get();
        if (threadContext == null) {
            threadContext = new ThreadContext();
            threadLocal.set(threadContext);
        }
        return threadContext;
    }

    public void finalizeTxn() {
        getCallContext().finalizeTransaction();
    }

    public Transaction getTransaction() {
        return getCallContext().getTransaction();
    }

    public long getTxnId() {
        return getCallContext().getTxnId();
    }

    public FactoryImpl getCurrentFactory() {
        return currentFactory;
    }

    public void setCurrentFactory(FactoryImpl currentFactory) {
        this.currentFactory = currentFactory;
    }

    public void reset() {
        finalizeTxn();
    }

    public byte[] toByteArray(Object obj) {
        return serializer.toByteArray(obj);
    }

    public Data toData(Object obj) {
        return serializer.writeObject(obj);
    }

    public Object toObject(Data data) {
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
        return getCallContext().isClient();
    }

    public int createNewThreadId() {
        return newThreadId.incrementAndGet();
    }

    public CallContext getCallContext() {
        return callContext;
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

    public int getThreadId() {
        return getCallContext().getThreadId();
    }

    public void setCallContext(CallContext callContext) {
        this.callContext = callContext;
    }
}
