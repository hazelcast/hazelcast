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

package com.hazelcast.internal.tpcengine.util;

import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.nio.NioReactor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class IntPromiseAllocatorTest {

    private Reactor reactor;
    private IntPromiseAllocator promiseAllocator;
    private int capacity = 1024;

    @Before
    public void before() {
        reactor = new NioReactor.Builder().build().start();
        promiseAllocator = new IntPromiseAllocator(reactor.eventloop(), capacity);
    }

    @After
    public void after() throws InterruptedException {
        terminate(reactor);
    }

    @Test
    public void testConstruction() {
        assertEquals(capacity, promiseAllocator.available());
    }

    @Test
    public void test_noMoreSpace() {
        for (int k = 0; k < capacity; k++) {
            IntPromise promise = promiseAllocator.allocate();
            assertNotNull(promise);
        }

        IntPromise promise = promiseAllocator.allocate();
        assertNull(promise);
    }

    @Test
    public void test_allocate_release_all() {
        List<IntPromise> promises = new ArrayList<>();
        int available = capacity;
        for (int k = 0; k < capacity; k++) {
            assertEquals(available, promiseAllocator.available());
            promises.add(promiseAllocator.allocate());
            available--;
        }

        assertEquals(0, promiseAllocator.available());

        for (int k = 0; k < capacity; k++) {
            IntPromise promise = promises.get(k);
            promiseAllocator.free(promise);
            available++;
            assertEquals(available, promiseAllocator.available());
        }
    }

    @Test
    public void test_pooling() {
        IntPromise promise = promiseAllocator.allocate();

        assertEquals(capacity - 1, promiseAllocator.available());

        assertEquals(1, promise.refCount);
        assertEquals(capacity - 1, promiseAllocator.available());

        promise.acquire();
        assertEquals(2, promise.refCount);
        assertEquals(capacity - 1, promiseAllocator.available());

        promise.release();
        assertEquals(1, promise.refCount);
        assertEquals(capacity - 1, promiseAllocator.available());

        promise.release();
        assertFalse(promise.isDone());
        assertEquals(capacity, promiseAllocator.available());
    }
}
