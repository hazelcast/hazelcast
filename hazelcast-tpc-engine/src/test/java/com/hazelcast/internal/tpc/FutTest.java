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

package com.hazelcast.internal.tpc;

import com.hazelcast.internal.tpc.nio.NioEventloop;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.tpc.TpcTestSupport.assertOpenEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class FutTest {

    private NioEventloop eventloop;
    private FutAllocator futAllocator;

    @Before
    public void before() {
        eventloop = new NioEventloop();
        eventloop.start();

        futAllocator = new FutAllocator(eventloop, 1024);
    }

    @After
    public void after() throws InterruptedException {
        if (eventloop != null) {
            eventloop.shutdown();
            assertTrue(eventloop.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void test_pooling() {
        Fut fut = new Fut(eventloop);

        fut.allocator = futAllocator;

        assertEquals(1, fut.refCount);
        assertEquals(0, futAllocator.size());

        fut.acquire();
        assertEquals(2, fut.refCount);
        assertEquals(0, futAllocator.size());

        fut.release();
        assertEquals(1, fut.refCount);
        assertEquals(0, futAllocator.size());

        fut.release();
        assertFalse(fut.isDone());
        assertEquals(1, futAllocator.size());
    }

    @Test
    public void test_thenOnCompletedFuture() {
        Fut fut = new Fut(eventloop);

        String result = "foobar";
        fut.complete(result);

        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference valueRef = new AtomicReference();
        AtomicReference throwableRef = new AtomicReference();

        fut.then((BiConsumer<Object, Throwable>) (o, throwable) -> {
            valueRef.set(o);
            throwableRef.set(throwable);
            executed.countDown();
        });

        assertOpenEventually(executed);
        assertSame(result, valueRef.get());
        assertNull(throwableRef.get());
    }

    @Test(expected = NullPointerException.class)
    public void test_completeExceptionallyWhenNull() {
        Fut future = new Fut(eventloop);

        future.completeExceptionally(null);
    }

    @Test
    public void test_completeExceptionally() {
        Fut fut = new Fut(eventloop);

        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference valueRef = new AtomicReference();
        AtomicReference throwableRef = new AtomicReference();
        fut.then((BiConsumer<Object, Throwable>) (o, throwable) -> {
            valueRef.set(o);
            throwableRef.set(throwable);
            executed.countDown();
        });

        Exception exception = new Exception();
        fut.completeExceptionally(exception);

        assertOpenEventually(executed);
        assertTrue(fut.isCompletedExceptionally());
        assertNull(valueRef.get());
        assertSame(exception, throwableRef.get());
    }


    @Test(expected = IllegalStateException.class)
    public void test_completeExceptionally_whenAlreadyCompleted() {
        Fut fut = new Fut(eventloop);

        fut.completeExceptionally(new Throwable());
        fut.completeExceptionally(new Throwable());
    }

    @Test
    public void test_complete() {
        Fut fut = new Fut(eventloop);

        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference valueRef = new AtomicReference();
        AtomicReference throwableRef = new AtomicReference();
        fut.then((BiConsumer<Object, Throwable>) (o, throwable) -> {
            valueRef.set(o);
            throwableRef.set(throwable);
            executed.countDown();
        });

        String result = "foobar";
        fut.complete(result);

        assertOpenEventually(executed);
        assertSame(result, valueRef.get());
        assertNull(throwableRef.get());
    }

    @Test(expected = IllegalStateException.class)
    public void test_complete_whenAlreadyCompleted() {
        Fut fut = new Fut(eventloop);

        fut.complete("first");
        fut.complete("second");
    }
}
