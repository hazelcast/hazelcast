/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.flakeidgen;

import com.hazelcast.util.function.Supplier;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class FlakeIdConcurrencyTestUtil {
    public static final int NUM_THREADS = 4;
    public static final int IDS_IN_THREAD = 100000;

    public static Set<Long> concurrentlyGenerateIds(final Supplier<Long> generator) throws Exception {
        final Thread[] threads = new Thread[NUM_THREADS];
        final CountDownLatch latch = new CountDownLatch(1);
        final Set<Long> ids = new HashSet<Long>();
        final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        Set<Long> localIds = new HashSet<Long>(IDS_IN_THREAD);
                        latch.await();
                        for (int i = 0; i < IDS_IN_THREAD; i++) {
                            localIds.add(generator.get());
                        }
                        synchronized (ids) {
                            ids.addAll(localIds);
                        }
                    } catch (Throwable e) {
                        error.compareAndSet(null, e);
                    }
                }
            };
            threads[i].start();
        }

        latch.countDown();

        for (Thread thread : threads) {
            thread.join();
        }

        assertNull(error.get());
        assertEquals(NUM_THREADS * IDS_IN_THREAD, ids.size());
        return ids;
    }
}
