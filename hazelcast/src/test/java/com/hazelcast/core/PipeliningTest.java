/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestThread;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PipeliningTest extends HazelcastTestSupport {

    private HazelcastInstance hz;

    @Before
    public void before() {
        hz = createHazelcastInstance();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_whenNegativeDepth() {
        new Pipelining<String>(0);
    }

    @Test(expected = NullPointerException.class)
    public void add_whenNull() throws InterruptedException {
        Pipelining<String> pipelining = new Pipelining<>(1);
        pipelining.add(null);
    }


    @Test
    public void testInterrupt() throws Exception {
        final Pipelining<String> pipelining = new Pipelining<>(1);
        pipelining.add(mock(CompletionStage.class));

        TestThread t = new TestThread() {
            @Override
            public void doRun() throws Throwable {
                pipelining.add(mock(CompletionStage.class));
            }
        };
        t.start();
        t.interrupt();
        t.assertFailsEventually(InterruptedException.class);
    }

    @Test
    public void testSpuriousWakeup() throws Exception {
        final Pipelining<String> pipelining = new Pipelining<>(1);
        pipelining.add(mock(CompletionStage.class));

        TestThread t = new TestThread() {
            @Override
            public void doRun() throws Throwable {
                pipelining.add(mock(CompletionStage.class));
            }
        };
        t.start();

        for (int k = 0; k < 100; k++) {
            Thread.sleep(5);
            LockSupport.unpark(t);
        }

        t.interrupt();
        t.assertFailsEventually(InterruptedException.class);
    }

    @Test
    public void test() throws Exception {
        int maxValue = 10_000;
        List<Integer> expected = new ArrayList<>();
        Map<Integer, Integer> entriesToAdd = new HashMap<>();

        // Populate data structures
        Random random = new Random();
        IntStream.range(0, maxValue)
                .forEach(i -> {
                    int value = random.nextInt();
                    entriesToAdd.put(i, value);
                    expected.add(value);
                });
        // Populate IMap
        IMap<Integer, Integer> map = hz.getMap("map");
        map.putAll(entriesToAdd);

        Pipelining<Integer> pipelining = new Pipelining<>(1);
        for (int index = 0; index < maxValue; index++) {
            pipelining.add(map.getAsync(index));
        }

        assertEquals(expected, pipelining.results());
    }
}
