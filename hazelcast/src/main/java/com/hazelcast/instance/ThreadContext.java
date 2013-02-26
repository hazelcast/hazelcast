/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.logging.Logger;
import com.hazelcast.spi.Operation;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

public final class ThreadContext {

    private static final ConcurrentMap<Thread, ThreadContext> contexts = new ConcurrentHashMap<Thread, ThreadContext>(1000);

    public static ThreadContext get() {
        Thread currentThread = Thread.currentThread();
        return contexts.get(currentThread);
    }

    public static ThreadContext getOrCreate() {
        Thread currentThread = Thread.currentThread();
        ThreadContext threadContext = contexts.get(currentThread);
        if (threadContext == null) {
            try {
                threadContext = new ThreadContext(currentThread);
                contexts.put(currentThread, threadContext);
                Iterator<Entry<Thread, ThreadContext>> threads = contexts.entrySet().iterator();
                while (threads.hasNext()) {
                    Entry<Thread, ThreadContext> entry = threads.next();
                    if (!entry.getKey().isAlive()) {
                        entry.getValue().destroy();
                        threads.remove();
                    }
                }
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
                throw e;
            }
            if (contexts.size() > 1000) {
                final String msg = contexts.size()
                        + " ThreadContexts are created!! You might have too many threads. Is that normal?";
                Logger.getLogger(ThreadContext.class.getName()).log(Level.WARNING, msg);
            }
        }
        return threadContext;
    }

    public static int getThreadId() {
        return (int) Thread.currentThread().getId();  // TODO: @mm - thread-id is truncated from native thread id
    }

//    public static TransactionImpl getTransaction(String name) {
//        ThreadContext ctx = get();
//        return ctx != null ? ctx.transactions.get(name) : null;
//    }
//
//    public static void setTransaction(String name, TransactionImpl transaction) {
//        ThreadContext ctx = getOrCreate();
//        final TransactionImpl current = ctx.transactions.get(name);
//        if (current != null && current != transaction) {
//            throw new IllegalStateException("Current thread has already an ongoing transaction!");
//        }
//        ctx.transactions.put(name, transaction);
//    }
//
//    public static TransactionImpl createOrGetTransaction(HazelcastInstanceImpl hazelcastInstance) {
//        ThreadContext ctx = getOrCreate();
//        TransactionImpl tx = ctx.transactions.get(hazelcastInstance.getName());
//        if (tx == null) {
////            tx = new TransactionImpl(hazelcastInstance);
//            ctx.transactions.put(hazelcastInstance.getName(), tx);
//        }
//        return tx;
//    }
//
//    public static void finalizeTransaction(String name) {
//        ThreadContext ctx = get();
//        if (ctx != null) {
//            ctx.transactions.remove(name);
//        }
//    }

    public static void shutdownAll() {
        contexts.clear();
    }

    public static void shutdown(Thread thread) {
        ThreadContext threadContext = contexts.remove(thread);
        if (threadContext != null) {
            threadContext.destroy();
        }
    }

    private final Thread thread;

//    private final Map<String, TransactionImpl> transactions = new HashMap<String, TransactionImpl>(2);

    private Operation currentOperation;

    private String callerUuid;

    public ThreadContext(Thread thread) {
        this.thread = thread;
    }

    public Operation getCurrentOperation() {
        return currentOperation;
    }

    public void setCurrentOperation(Operation currentOperation) {
        this.currentOperation = currentOperation;
    }

    public String getCallerUuid() {
        return callerUuid;
    }

    public void setCallerUuid(String callerUuid) {
        this.callerUuid = callerUuid;
    }

    private void destroy() {
//        transactions.clear();
    }

    public Thread getThread() {
        return thread;
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
