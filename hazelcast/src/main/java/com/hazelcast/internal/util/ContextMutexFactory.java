/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides reference-counted mutexes suitable for synchronization in the context of a supplied object.
 * <p>
 * Context objects and their associated mutexes are stored in a {@link Map}. Client code is responsible to invoke
 * {@link Mutex#close()} on the obtained {@link Mutex} after having synchronized on the mutex; failure to do so
 * will leave an entry residing in the internal {@link Map} which may have adverse effects on the ability to garbage
 * collect the context object and the mutex.
 * <p>
 * The returned {@link Mutex}es implement {@link Closeable}, so can be conveniently used in a try-with-resources statement.
 * <p>
 * Typical usage would allow, for example, synchronizing access to a {@link java.util.concurrent.ConcurrentMap} on a
 * per-key basis, to avoid blocking other threads which could perform updates on other entries of the {@link Map}.
 *
 * <pre>{@code
 *    class Test {
 *
 *        private final ContextMutexFactory mutexFactory = new ContextMutexFactory();
 *        private final ConcurrentMap<String, String> mapToSync = new ConcurrentHashMap<String, String>();
 *
 *        public String getValueForKey(String key) {
 *            ContextMutexFactory.Mutex mutex = mutexFactory.mutexFor(key);
 *            try {
 *                synchronized (mutex) {
 *                    // critical section - this thread is the only one trying to add value under this key
 *                    String value = mapToSync.get(key);
 *                    if (value == null) {
 *                        value = ...; // compute the value
 *                        mapToSync.put(key, value);
 *                    }
 *                    return value;
 *                }
 *            } finally {
 *                mutex.close();
 *            }
 *        }
 *    }
 * }</pre>
 */
public final class ContextMutexFactory {

    final Map<Object, Mutex> mutexMap = new HashMap<Object, Mutex>();

    // synchronizes access to mutexMap and Mutex.referenceCount
    private final Object mainMutex = new Object();

    public Mutex mutexFor(Object mutexKey) {
        Mutex mutex;
        synchronized (mainMutex) {
            mutex = mutexMap.get(mutexKey);
            if (mutex == null) {
                mutex = new Mutex(mutexKey);
                mutexMap.put(mutexKey, mutex);
            }
            mutex.referenceCount++;
        }
        return mutex;
    }

    /**
     * Reference counted mutex, which will remove itself from the mutexMap when it is no longer referenced.
     */
    public final class Mutex implements Closeable {

        private final Object key;

        private int referenceCount;

        private Mutex(Object key) {
            this.key = key;
        }

        @Override
        public void close() {
            synchronized (mainMutex) {
                referenceCount--;
                if (referenceCount == 0) {
                    mutexMap.remove(key);
                }
            }
        }
    }
}
