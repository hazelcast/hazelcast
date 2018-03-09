/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.util.HashUtil.hashToIndex;

/**
 * Utility methods to getOrPutSynchronized and getOrPutIfAbsent in a thread safe way
 * from a {@link ConcurrentMap} with a {@link ConstructorFunction}.
 */
public final class ConcurrencyUtil {

    private final static Object[] LOCK_STRIPE = new Object[Runtime.getRuntime().availableProcessors() * 20];

    private ConcurrencyUtil() {
    }

    /**
     * Atomically sets the max value.
     *
     * If the current value is larger than the provided value, the call is ignored.
     * So it will not happen that a smaller value will overwrite a larger value.
     */
    public static <E> void setMax(E obj, AtomicLongFieldUpdater<E> updater, long value) {
        for (; ; ) {
            long current = updater.get(obj);
            if (current >= value) {
                return;
            }

            if (updater.compareAndSet(obj, current, value)) {
                return;
            }
        }
    }

    public static boolean setIfGreaterThan(AtomicLong oldValue, long newValue) {
        while (true) {
            long local = oldValue.get();
            if (newValue <= local) {
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

    public static Object getLock(Object mutexKey) {
        if (mutexKey == null) {
            return LOCK_STRIPE[0];
        }

        return LOCK_STRIPE[hashToIndex(mutexKey.hashCode(), LOCK_STRIPE.length)];
    }

    public static void executeUnderMutex(Object mutexKey, Runnable task) {
        synchronized (getLock(mutexKey)) {
            task.run();
        }
    }

    public static <E> E executeUnderMutex(Object mutexKey, Callable<E> task) {
        try {
            synchronized (getLock(mutexKey)) {
                return task.call();
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <K, V> V getOrPutSynchronized(ConcurrentMap<K, V> map, K key, ConstructorFunction<K, V> func) {
        V value = map.get(key);
        if (value == null) {
            synchronized (getLock(key)) {
                value = map.get(key);
                if (value == null) {
                    value = func.createNew(key);
                    map.put(key, value);
                }
            }
        }
        return value;
    }

    public static <K, V> V getOrPutIfAbsent(ConcurrentMap<K, V> map, K key, ConstructorFunction<K, V> func) {
        V value = map.get(key);
        if (value == null) {
            value = func.createNew(key);
            V current = map.putIfAbsent(key, value);
            value = current == null ? value : current;
        }
        return value;
    }

}
