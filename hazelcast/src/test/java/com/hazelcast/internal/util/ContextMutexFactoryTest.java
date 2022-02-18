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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test basic lock operation of {@link ContextMutexFactory}.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class ContextMutexFactoryTest {

    private ContextMutexFactory contextMutexFactory;
    private final AtomicBoolean testFailed = new AtomicBoolean(false);

    @Before
    public void setup() {
        contextMutexFactory = new ContextMutexFactory();
    }

    @Test
    public void testConcurrentMutexOperation() {
        final String[] keys = new String[]{"a", "b", "c"};
        final Map<String, Integer> timesAcquired = new HashMap<String, Integer>();

        int concurrency = RuntimeAvailableProcessors.get() * 3;
        final CyclicBarrier cyc = new CyclicBarrier(concurrency + 1);

        for (int i = 0; i < concurrency; i++) {
            new Thread(new Runnable() {

                @Override
                public void run() {
                    await(cyc);

                    for (String key : keys) {
                        ContextMutexFactory.Mutex mutex = contextMutexFactory.mutexFor(key);
                        try {
                            synchronized (mutex) {
                                Integer value = timesAcquired.get(key);
                                if (value == null) {
                                    timesAcquired.put(key, 1);
                                } else {
                                    timesAcquired.put(key, value + 1);
                                }
                            }
                        } finally {
                            mutex.close();
                        }
                    }

                    await(cyc);
                }
            }).start();
        }
        // start threads, wait for them to finish
        await(cyc);
        await(cyc);

        // assert each key's lock was acquired by each thread
        for (String key : keys) {
            assertEquals(concurrency, timesAcquired.get(key).longValue());
        }
        // assert there are no mutexes leftover
        assertEquals(0, contextMutexFactory.mutexMap.size());

        if (testFailed.get()) {
            fail("Failure due to exception while waiting on cyclic barrier.");
        }
    }

    private void await(CyclicBarrier cyc) {
        try {
            cyc.await();
        } catch (InterruptedException e) {
            testFailed.set(true);
        } catch (BrokenBarrierException e) {
            testFailed.set(true);
        }
    }
}
