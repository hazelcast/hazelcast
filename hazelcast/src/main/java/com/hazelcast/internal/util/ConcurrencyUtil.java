/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import java.lang.invoke.VarHandle;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Utility methods to getOrPutSynchronized and getOrPutIfAbsent in a thread safe way
 * from a {@link ConcurrentMap} with a {@link ConstructorFunction}.
 */
public final class ConcurrencyUtil {

    /**
     * The Caller runs executor is an Executor that executes the task on the calling thread.
     * This is useful when an Executor is required, but offloading to a different thread
     * is very costly, and it is faster to run on the calling thread.
     */
    public static final Executor CALLER_RUNS = new Executor() {
        @Override
        public void execute(Runnable command) {
            command.run();
        }

        @Override
        public String toString() {
            return "CALLER_RUNS";
        }
    };

    // Default executor for async callbacks: ForkJoinPool.commonPool() or a thread-per-task executor when
    // the common pool does not support parallelism
    private static Executor defaultAsyncExecutor;

    static {
        Executor asyncExecutor;
        if (ForkJoinPool.getCommonPoolParallelism() > 1) {
            asyncExecutor = ForkJoinPool.commonPool();
        } else {
            asyncExecutor = command -> new Thread(command).start();
        }
        defaultAsyncExecutor = asyncExecutor;
    }

    private ConcurrencyUtil() {
    }

    /**
     * WARNING this method should not be called from static context.
     */
    public static Executor getDefaultAsyncExecutor() {
        return defaultAsyncExecutor;
    }

    /**
     * Used by AbstractHazelcastClassRunner to override default executor.
     */
    public static void setDefaultAsyncExecutor(Executor executor) {
        defaultAsyncExecutor = executor;
    }

    /**
     * Atomically sets the max value.
     * <p>
     * If the current value is larger than the provided value, the call is ignored.
     * So it will not happen that a smaller value will overwrite a larger value.
     */
    public static <E> void setMax(E obj, VarHandle handle, long value) {
        for (; ; ) {
            long current = (long) handle.get(obj);
            if (current >= value) {
                return;
            }

            if (handle.compareAndSet(obj, current, value)) {
                return;
            }
        }
    }

    public static boolean setIfEqualOrGreaterThan(AtomicLong oldValue, long newValue) {
        while (true) {
            long local = oldValue.get();
            if (newValue < local) {
                return false;
            }
            if (oldValue.compareAndSet(local, newValue)) {
                return true;
            }
        }
    }

    public static <K, V> V getOrPutSynchronized(ConcurrentMap<K, V> map, K key, final Object mutex,
                                                ConstructorFunction<K, V> func) {
        if (mutex == null) {
            throw new NullPointerException();
        }
        V value = map.get(key);
        if (value == null) {
            synchronized (mutex) {
                value = map.get(key);
                if (value == null) {
                    value = func.createNew(key);
                    map.put(key, value);
                }
            }
        }
        return value;
    }

    public static <K, V> V getOrPutSynchronized(ConcurrentMap<K, V> map, K key, ContextMutexFactory contextMutexFactory,
                                                ConstructorFunction<K, V> func) {
        if (contextMutexFactory == null) {
            throw new NullPointerException();
        }
        V value = map.get(key);
        if (value == null) {
            ContextMutexFactory.Mutex mutex = contextMutexFactory.mutexFor(key);
            try {
                synchronized (mutex) {
                    value = map.get(key);
                    if (value == null) {
                        value = func.createNew(key);
                        map.put(key, value);
                    }
                }
            } finally {
                mutex.close();
            }
        }
        return value;
    }

    /**
     * Returns the {@code value} corresponding to {@code key} in the {@code map}. If {@code key} is not mapped in {@code map},
     * then a {@code value} is computed using {@code func}, inserted into the map and returned.
     * <p>
     * The behavior is equivalent to {@link ConcurrentMap#computeIfAbsent(Object, Function)}, with the following exceptions:
     * <ul>
     * <li>If no mapping, the value of {@code func.createNew(K)} will be inserted into the {@code map} - even if {@code null}
     * <li>Instances of {@link ConcurrentMap} can override their implementation, but here the implementation can be assured
     * </ul>
     * <p>
     * The typical use case of this function over {@link ConcurrentMap#computeIfAbsent(Object, Function)} would be in the case of
     * a {@link ConcurrentHashMap}, where the implementation is overridden with the following guarantee:<br>
     * "The supplied function is invoked exactly once per invocation of this method if the key is absent, else not at all. Some
     * attempted update operations on this map by other threads may be blocked while computation is in progress"
     * <p>
     * By comparison, this implementation cannot guarantee {@code func.createNew(K)} will be executed a maximum of once, because
     * no blocking if performed - if the {@code key} is absent from the {@code map}, and this method is called concurrently,
     * {@code func.createNew(K)} may be invoked multiple times before any new value is inserted.
     * <p>
     * This is a performance tradeoff - improve {@code map} throughput and reduce deadlocks, at the expense of redundant object
     * instantiation.
     */
    public static <K, V> V getOrPutIfAbsent(Map<K, V> map, K key, ConstructorFunction<K, V> func) {
        V value = map.get(key);
        if (value == null) {
            value = func.createNew(key);
            V current = map.putIfAbsent(key, value);
            value = current == null ? value : current;
        }
        return value;
    }

}
