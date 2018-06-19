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

import com.hazelcast.core.IFunction;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SampleableConcurrentHashMapTest extends HazelcastTestSupport {

    private static final int ENTRY_COUNT = 100;
    private static final int SAMPLE_COUNT = 15;
    private static final int COUNT = 10;

    @Test
    public void samplesSuccessfullyRetrieved() {
        SampleableConcurrentHashMap<Integer, Integer> sampleableConcurrentHashMap =
                new SampleableConcurrentHashMap<Integer, Integer>(ENTRY_COUNT);

        for (int i = 0; i < ENTRY_COUNT; i++) {
            sampleableConcurrentHashMap.put(i, i);
        }

        Iterable<SampleableConcurrentHashMap.SamplingEntry<Integer, Integer>> samples =
                sampleableConcurrentHashMap.getRandomSamples(SAMPLE_COUNT);
        assertNotNull(samples);

        int sampleCount = 0;
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        for (SampleableConcurrentHashMap.SamplingEntry<Integer, Integer> sample : samples) {
            map.put(sample.getEntryKey(), sample.getEntryValue());
            sampleCount++;
        }
        // Sure that there is enough sample as we expected
        assertEquals(SAMPLE_COUNT, sampleCount);
        // Sure that all samples are different
        assertEquals(SAMPLE_COUNT, map.size());
    }

    @Test
    public void applyIfAbsentTest() throws Throwable {
        final SampleableConcurrentHashMap<String, String> map =
                new SampleableConcurrentHashMap<String, String>(10);

        assertEquals(map.applyIfAbsent("key", new IFunction<String, String>() {
            @Override
            public String apply(String input) {
                return "value";
            }
        }), "value");

        final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
        final CountDownLatch latch = new CountDownLatch(COUNT);

        for (int i = 0; i < COUNT; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        assertEquals(map.applyIfAbsent("key", new IFunction<String, String>() {
                            @Override
                            public String apply(String input) {
                                return "value1";
                            }
                        }), "value");
                    } catch (Throwable e) {
                        error.set(e);
                    } finally {
                        latch.countDown();
                    }
                }
            }).start();
        }

        latch.await(20, TimeUnit.SECONDS);

        if (error.get() != null) {
            throw error.get();
        }

        map.clear();

        map.applyIfAbsent("key", new IFunction<String, String>() {
            @Override
            public String apply(String input) {
                return null;
            }
        });

        assertEquals(map.size(), 0);
    }
}
