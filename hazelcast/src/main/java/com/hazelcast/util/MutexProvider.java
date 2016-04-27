/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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


import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

/**
 * A provider of fine-grained mutexes, that allows per-key locking on {@code Map}s. The {@link Mutex}es returned by
 * this class implement {@link Closeable}, you should make sure you invoke {@link Mutex#close()} after using the mutex
 * to allow cleanup of the internal mutexes map. Typical usage:
 * <pre>
 *     class Test {
 *
 *         private static final MutexProvider mutexProvider = new MutexProvider();
 *         private Map&lt;String, String&gt; mapToSync = new HashMap&lt;String, String&gt;();
 *
 *         public void test(String key, String value) {
 *             // critical section
 *             MutexProvider.Mutex mutex = mutexProvider.get(key);
 *             try {
 *                 synchronized (mutex) {
 *                      if (mapToSync.get(key) == null) {
 *                          mapToSync.put(key, value);
 *                      }
 *                 }
 *             }
 *             finally {
 *                 mutex.close();
 *             }
 *         }
 *     }
 * </pre>
 *
 */
public class MutexProvider {
    Map<Object, Mutex> mutexMap = new HashMap<Object, Mutex>();
    // synchronizes access to mutexMap and Mutex.referenceCount
    private Object mainMutex = new Object();

    public synchronized Mutex getMutex(Object mutexKey) {
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
