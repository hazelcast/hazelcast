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
import com.hazelcast.internal.tpcengine.TpcTestSupport;
import com.hazelcast.internal.tpcengine.nio.NioReactor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertOpenEventually;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class PromiseTest {

    private Reactor reactor;
    private PromiseAllocator promiseAllocator;

    @Before
    public void before() {
        reactor = new NioReactor.Builder().build().start();
        promiseAllocator = new PromiseAllocator(reactor.eventloop(), 1024);
    }

    @After
    public void after() throws InterruptedException {
        TpcTestSupport.terminate(reactor);
    }

    @Test
    public void test_thenOnCompletedFuture() {
        Promise promise = new Promise(reactor.eventloop());

        String result = "foobar";
        promise.complete(result);

        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference valueRef = new AtomicReference();
        AtomicReference throwableRef = new AtomicReference();

        promise.then((BiConsumer<Object, Throwable>) (o, throwable) -> {
            valueRef.set(o);
            throwableRef.set(throwable);
            executed.countDown();
        });

        assertOpenEventually(executed);
        assertSame(result, valueRef.get());
        assertNull(throwableRef.get());
    }

    @Test
    public void test_completeExceptionallyWhenNull() {
        Promise promise = new Promise(reactor.eventloop());

        assertThrows(NullPointerException.class, () -> promise.completeExceptionally(null));
    }

    @Test
    public void test_completeExceptionally() {
        Promise promise = new Promise(reactor.eventloop());

        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference valueRef = new AtomicReference();
        AtomicReference throwableRef = new AtomicReference();
        promise.then((BiConsumer<Object, Throwable>) (o, throwable) -> {
            valueRef.set(o);
            throwableRef.set(throwable);
            executed.countDown();
        });

        Exception exception = new Exception();
        promise.completeExceptionally(exception);

        assertOpenEventually(executed);
        assertTrue(promise.isCompletedExceptionally());
        assertNull(valueRef.get());
        assertSame(exception, throwableRef.get());
    }


    @Test
    public void test_completeExceptionally_whenAlreadyCompleted() {
        Promise promise = new Promise(reactor.eventloop());

        promise.completeExceptionally(new Throwable());

        assertThrows(IllegalStateException.class, () -> promise.completeExceptionally(new Throwable()));
    }

    @Test
    public void test_complete() {
        Promise promise = new Promise(reactor.eventloop());

        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference valueRef = new AtomicReference();
        AtomicReference throwableRef = new AtomicReference();
        promise.then((BiConsumer<Object, Throwable>) (o, throwable) -> {
            valueRef.set(o);
            throwableRef.set(throwable);
            executed.countDown();
        });

        String result = "foobar";
        promise.complete(result);

        assertOpenEventually(executed);
        assertSame(result, valueRef.get());
        assertNull(throwableRef.get());
    }

    @Test
    public void test_complete_whenAlreadyCompleted() {
        Promise promise = new Promise(reactor.eventloop());

        promise.complete("first");
        assertThrows(IllegalStateException.class, () -> promise.complete("second"));
    }
}
