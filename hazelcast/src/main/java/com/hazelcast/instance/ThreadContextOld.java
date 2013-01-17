///*
// * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.hazelcast.instance;
//
//import com.hazelcast.logging.ILogger;
//import com.hazelcast.logging.Logger;
//import com.hazelcast.transaction.TransactionImpl;
//
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.Map.Entry;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.logging.Level;
//
//public final class ThreadContextOld {
//
//    private static final ConcurrentMap<Thread, ThreadContextOld> mapContexts = new ConcurrentHashMap<Thread, ThreadContextOld>(1000);
//
//    public static ThreadContextOld onlyGet() { // new ThreadContext.get()!
//        Thread currentThread = Thread.currentThread();
//        return mapContexts.get(currentThread);
//    }
//
//    public static ThreadContextOld ensureAndGet() {
//        Thread currentThread = Thread.currentThread();
//        ThreadContextOld threadContext = mapContexts.get(currentThread);
//        if (threadContext == null) {
//            try {
//                threadContext = new ThreadContextOld(Thread.currentThread());
//                mapContexts.put(currentThread, threadContext);
//                Iterator<Entry<Thread, ThreadContextOld>> threads = mapContexts.entrySet().iterator();
//                while (threads.hasNext()) {
//                    Entry<Thread, ThreadContextOld> entry = threads.next();
//                    if (!entry.getKey().isAlive()) {
//                        entry.getValue().destroy();
//                        threads.remove();
//                    }
//                }
//            } catch (OutOfMemoryError e) {
//                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
//                throw e;
//            }
//            if (mapContexts.size() > 1000) {
//                String msg = " ThreadContext is created!! You might have too many threads. Is that normal?";
//                Logger.getLogger(ThreadContextOld.class.getName()).log(Level.WARNING, mapContexts.size() + msg);
//            }
//        }
//        return threadContext;
//    }
//
//    public static TransactionImpl getTransaction(String name) {
//        ThreadContextOld ctx = onlyGet();
//        return ctx != null ? ctx.transactions.get(name) : null;
//    }
//
//    public static TransactionImpl createTransaction(HazelcastInstanceImpl hazelcastInstance) {
//        ThreadContextOld ctx = ensureAndGet();
//        TransactionImpl tx = ctx.transactions.get(hazelcastInstance.getName());
//        if (tx == null) {
//            tx = new TransactionImpl(hazelcastInstance);
//            ctx.transactions.put(hazelcastInstance.getName(), tx);
//        }
//        return tx;
//    }
//
//    private static final AtomicInteger newThreadId = new AtomicInteger();
//
//    public static ThreadContextOld get() {
//        Thread currentThread = Thread.currentThread();
//        ThreadContextOld threadContext = mapContexts.get(currentThread);
//        if (threadContext == null) {
//            try {
//                threadContext = new ThreadContextOld(Thread.currentThread());
//                mapContexts.put(currentThread, threadContext);
//                Iterator<Entry<Thread, ThreadContextOld>> threads = mapContexts.entrySet().iterator();
//                while (threads.hasNext()) {
//                    Entry<Thread, ThreadContextOld> entry = threads.next();
//                    if (!entry.getKey().isAlive()) {
//                        entry.getValue().destroy();
//                        threads.remove();
//                    }
//                }
//            } catch (OutOfMemoryError e) {
//                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
//                throw e;
//            }
//            if (mapContexts.size() > 1000) {
//                String msg = " ThreadContext is created!! You might have too many threads. Is that normal?";
//                Logger.getLogger(ThreadContextOld.class.getName()).log(Level.WARNING, mapContexts.size() + msg);
//            }
//        }
//        return threadContext;
//    }
//
//    public static int createNewThreadId() {
//        return newThreadId.incrementAndGet();
//    }
//
//    public static void shutdownAll() {
//        mapContexts.clear();
//    }
//
//    public static synchronized void shutdown(Thread thread) {
//        ThreadContextOld threadContext = mapContexts.remove(thread);
//        if (threadContext != null) {
//            threadContext.destroy();
//        }
//    }
//
//    private final Thread thread;
//
//    private final Map<String, HazelcastInstanceThreadContext> mapHazelcastInstanceContexts
//            = new HashMap<String, HazelcastInstanceThreadContext>(2);
//
//    private final Map<String, TransactionImpl> transactions = new HashMap<String, TransactionImpl>(2);
//
//    private HazelcastInstanceImpl currentInstance;
//
//    private Object currentOperation = null;
//
//    private ThreadContextOld(Thread thread) {
//        this.thread = thread;
//    }
//
//    public void finalizeTransaction() {
//        getCallContext().finalizeTransaction();
//    }
//
//    public TransactionImpl getTransaction() {
//        return getCallContext().getTransaction();
//    }
//
//    public HazelcastInstanceImpl getCurrentInstance() {
//        return currentInstance;
//    }
//
//    public void setCurrentInstance(HazelcastInstanceImpl instance) {
//        this.currentInstance = instance;
//    }
//
//    public void reset() {
//        finalizeTransaction();
//    }
//
//    public HazelcastInstanceThreadContext getHazelcastInstanceThreadContext(HazelcastInstanceImpl instance) {
//        if (instance == null) {
//            ILogger logger = Logger.getLogger(ThreadContextOld.class.getName());
//            logger.log(Level.SEVERE, "HazelcastInstance is null", new Throwable());
//        }
//        final String factoryKey = instance != null ? instance.getName() : "null";
//        HazelcastInstanceThreadContext hic = mapHazelcastInstanceContexts.get(factoryKey);
//        if (hic != null) return hic;
//        hic = new HazelcastInstanceThreadContext(instance);
//        mapHazelcastInstanceContexts.put(factoryKey, hic);
//        return hic;
//    }
//
//    /**
//     * Is this thread remote Java or CSharp Client thread?
//     *
//     * @return true if the thread is for Java or CSharp Client, false otherwise
//     */
//    public boolean isClient() {
//        return getCallContext().isClient();
//    }
//
//    public void setCallContext(CallContext callContext) {
//        getHazelcastInstanceThreadContext(currentInstance).setCallContext(callContext);
//    }
//
//    public CallContext getCallContext() {
//        return getHazelcastInstanceThreadContext(currentInstance).getCallContext();
//    }
//
//    public int getThreadId() {
//        return getCallContext().getThreadId();
//    }
//
//    public Object getCurrentOperation() {
//        return currentOperation;
//    }
//
//    public void setCurrentOperation(Object currentOperation) {
//        this.currentOperation = currentOperation;
//    }
//
//    public void shutdown(HazelcastInstanceImpl instance) {
//        mapHazelcastInstanceContexts.remove(instance.getName());
//        currentInstance = null;
//    }
//
//    private void destroy() {
//        mapHazelcastInstanceContexts.clear();
//        currentInstance = null;
//    }
//
//    private class HazelcastInstanceThreadContext {
//        final HazelcastInstanceImpl instance;
//        volatile CallContext callContext = null;
//
//        HazelcastInstanceThreadContext(HazelcastInstanceImpl instance) {
//            this.instance = instance;
//            callContext = (new CallContext(createNewThreadId(), false));
//        }
//
//        CallContext getCallContext() {
//            return callContext;
//        }
//
//        void setCallContext(CallContext callContext) {
//            this.callContext = callContext;
//        }
//    }
//
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        ThreadContextOld that = (ThreadContextOld) o;
//        if (thread != null ? !thread.equals(that.thread) : that.thread != null) return false;
//        return true;
//    }
//
//    @Override
//    public int hashCode() {
//        return thread != null ? thread.hashCode() : 0;
//    }
//}
