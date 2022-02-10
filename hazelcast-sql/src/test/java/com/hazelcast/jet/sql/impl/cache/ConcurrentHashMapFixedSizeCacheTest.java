/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.sql.impl.cache;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConcurrentHashMapFixedSizeCacheTest {
    @Test
    public void when_addingElementsToCacheInSingleThread_then_properSizeAndElements() {
        int maxCapacity = 20;
        int elementsToAdd = 100;
        ConcurrentHashMapFixedSizeCache<Integer, Integer> lru = new ConcurrentHashMapFixedSizeCache<>(maxCapacity);
        for (int i = 0; i < elementsToAdd; i++) {
            lru.computeIfAbsent(i, Function.identity());
        }

        assertEquals(maxCapacity, lru.cache.size());
        for (int i = 0; i < maxCapacity; i++) {
            assertTrue(lru.cache.containsKey(i));
        }
    }

    @Test
    public void when_addingElementsToCacheMultiThreaded_then_minProperSizeAndElements() {
        int maxCapacity = 20;
        int elementsToAdd = 100;
        int threadCount = 10;
        ConcurrentHashMapFixedSizeCache<Integer, Integer> lru = new ConcurrentHashMapFixedSizeCache<>(maxCapacity);
        Runnable runnable = () -> {
            for (int i = 0; i < elementsToAdd; i++) {
                lru.computeIfAbsent(i, Function.identity());
            }
        };

        List<Thread> threadList = IntStream.range(0, threadCount)
                .mapToObj(value -> new Thread(runnable))
                .collect(Collectors.toList());
        threadList.forEach(Thread::start);
        threadList.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        assertTrue(lru.cache.size() >= maxCapacity);
        for (int i = 0; i < maxCapacity; i++) {
            assertTrue(lru.cache.containsKey(i));
        }
    }

    @Test
    public void when_creatingEmptyCache_then_fail() {
        assertThrows(IllegalArgumentException.class, () -> {
            ConcurrentHashMapFixedSizeCache<Integer, Integer> lru = new ConcurrentHashMapFixedSizeCache<>(0);
        });
    }
}
