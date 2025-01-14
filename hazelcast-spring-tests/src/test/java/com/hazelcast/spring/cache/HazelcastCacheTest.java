/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring.cache;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.spring.CustomSpringExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
 * Tests for {@link HazelcastCache}.
 *
 * @author Stephane Nicoll
 */
@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"simple-config.xml"})
class HazelcastCacheTest {

    @Autowired
    public CacheManager cacheManager;

    private Cache cache;

    @BeforeAll
    @AfterAll
    public static void start() {
        Hazelcast.shutdownAll();
    }

    @BeforeEach
    public void setup() {
        this.cache = cacheManager.getCache("test");
    }

    @Test
    void testCacheGetCallable() {
        doTestCacheGetCallable("test");
    }

    @Test
    void testCacheGetCallableWithNull() {
        doTestCacheGetCallable(null);
    }

    @SuppressWarnings("ConstantConditions")
    private void doTestCacheGetCallable(final Object returnValue) {
        String key = createRandomKey();

        assertNull(cache.get(key));
        Object value = cache.get(key, () -> returnValue);
        assertEquals(returnValue, value);
        assertEquals(value, cache.get(key).get());
    }

    @Test
    void testCacheGetCallableNotInvokedWithHit() {
        doTestCacheGetCallableNotInvokedWithHit("existing");
    }

    @Test
    void testCacheGetCallableNotInvokedWithHitNull() {
        doTestCacheGetCallableNotInvokedWithHit(null);
    }

    private void doTestCacheGetCallableNotInvokedWithHit(Object initialValue) {
        String key = createRandomKey();
        cache.put(key, initialValue);

        Object value = cache.get(key, () -> {
            throw new IllegalStateException("Should not have been invoked");
        });
        assertEquals(initialValue, value);
    }

    @Test
    void testCacheGetCallableFail() {
        String key = createRandomKey();
        assertNull(cache.get(key));

        try {
            cache.get(key, () -> {
                throw new UnsupportedOperationException("Expected exception");
            });
        } catch (Cache.ValueRetrievalException ex) {
            assertNotNull(ex.getCause());
            assertEquals(UnsupportedOperationException.class, ex.getCause().getClass());
        }
    }

    /**
     * Tests that a call to get with a Callable concurrently properly synchronize the invocations.
     */
    @Test
    void testCacheGetSynchronized() throws Exception {
        final AtomicInteger counter = new AtomicInteger();
        final List<Object> results = new CopyOnWriteArrayList<>();
        final CountDownLatch latch = new CountDownLatch(10);

        final String key = createRandomKey();
        Runnable run = () -> {
            try {
                Integer value = cache.get(key, () -> {
                    // make sure the thread will overlap
                    sleepMillis(50);

                    return counter.incrementAndGet();
                });
                results.add(value);
            } finally {
                latch.countDown();
            }
        };

        for (int i = 0; i < 10; i++) {
            new Thread(run).start();
        }
        latch.await();

        assertEquals(10, results.size());
        for (Object result : results) {
            assertThat((Integer) result).isEqualTo(1);
        }
    }

    @Test
    void testCacheRetrieveWithNull() {
        assertThrows(NullPointerException.class, () -> cache.retrieve(null));
    }

    @Test
    void testCacheRetrieveWithRandomKey() throws ExecutionException, InterruptedException {
        String key = createRandomKey();
        CompletableFuture<?> resultFuture = cache.retrieve(key);

        assertNotNull(resultFuture);
        assertNull(resultFuture.get());
    }

    @Test
    void testCacheRetrieveWithExistingKey() throws ExecutionException, InterruptedException {
        String key = createRandomKey();
        cache.put(key, "test");

        CompletableFuture<?> resultFuture = cache.retrieve(key);

        assertNotNull(resultFuture);
        assertThat(resultFuture.get()).isEqualTo("test");
    }

    private String createRandomKey() {
        return UUID.randomUUID().toString();
    }
}
