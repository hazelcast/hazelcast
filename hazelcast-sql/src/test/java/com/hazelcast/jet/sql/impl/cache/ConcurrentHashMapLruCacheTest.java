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
public class ConcurrentHashMapLruCacheTest {
    @Test
    public void when_addingElementsToLruCacheInSingleThread_then_properSizeAndElements() {
        int maxCapacity = 20;
        int elementsToAdd = 100;
        ConcurrentHashMapLruCache<Integer, Integer> lru = new ConcurrentHashMapLruCache<>(maxCapacity);
        for (int i = 0; i < elementsToAdd; i++) {
            lru.computeIfAbsent(i, Function.identity());
        }

        assertEquals(maxCapacity, lru.cache.size());
        assertEquals(maxCapacity, lru.keyQueue.size());
        for (int i = elementsToAdd - maxCapacity; i < elementsToAdd; i++) {
            assertTrue(lru.cache.containsKey(i));
        }
    }

    @Test
    public void when_addingElementsWithinFastPath_then_queueIsNotModified() {
        int maxCapacity = 20;
        int elementsToAdd = 19;
        ConcurrentHashMapLruCache<Integer, Integer> lru = new ConcurrentHashMapLruCache<>(maxCapacity, 19);
        for (int i = 0; i < elementsToAdd; i++) {
            lru.computeIfAbsent(i, Function.identity());
        }
        lru.computeIfAbsent(0, Function.identity());

        assertEquals(0, lru.keyQueue.peekFirst().intValue());
        assertEquals(elementsToAdd - 1, lru.keyQueue.peekLast().intValue());
    }

    @Test
    public void when_addingElementsNotWithinFastPath_then_queueIsModified() {
        int maxCapacity = 20;
        int elementsToAdd = 19;
        ConcurrentHashMapLruCache<Integer, Integer> lru = new ConcurrentHashMapLruCache<>(maxCapacity, 1);
        for (int i = 0; i < elementsToAdd; i++) {
            lru.computeIfAbsent(i, Function.identity());
        }
        lru.computeIfAbsent(0, Function.identity());

        assertEquals(1, lru.keyQueue.peekFirst().intValue());
        assertEquals(0, lru.keyQueue.peekLast().intValue());
    }

    @Test
    public void when_addingElementsToLruCacheMultiThreaded_then_properSize() {
        int maxCapacity = 20;
        int elementsToAdd = 100;
        int threadCount = 10;
        ConcurrentHashMapLruCache<Integer, Integer> lru = new ConcurrentHashMapLruCache<>(maxCapacity);
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

        assertEquals(maxCapacity, lru.cache.size());
        assertEquals(maxCapacity, lru.keyQueue.size());
    }

    @Test
    public void when_creatingEmptyCache_then_fail() {
        assertThrows(AssertionError.class, () -> {
            ConcurrentHashMapLruCache<Integer, Integer> lru = new ConcurrentHashMapLruCache<>(0);
        });
    }
}