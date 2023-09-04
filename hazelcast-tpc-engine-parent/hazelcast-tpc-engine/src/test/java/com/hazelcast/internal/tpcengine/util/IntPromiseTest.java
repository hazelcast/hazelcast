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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertOpenEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class IntPromiseTest {
    private Reactor reactor;

    @Before
    public void before() {
        reactor = new NioReactor.Builder().build().start();
    }

    @After
    public void after() throws InterruptedException {
        terminate(reactor);
    }

    @Test
    public void test_thenOnCompletedFuture() {
        IntPromise promise = new IntPromise(reactor.eventloop());

        int result = 10;
        promise.complete(result);

        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference<Integer> valueRef = new AtomicReference();
        AtomicReference<Throwable> throwableRef = new AtomicReference();

        promise.then((o, throwable) -> {
            valueRef.set(o);
            throwableRef.set(throwable);
            executed.countDown();
        });

        assertOpenEventually(executed);
        assertEquals(new Integer(result), valueRef.get());
        assertNull(throwableRef.get());
    }

    @Test
    public void test_completeExceptionallyWhenNull() {
        IntPromise promise = new IntPromise(reactor.eventloop());

        assertThrows(NullPointerException.class, () -> promise.completeExceptionally(null));
    }

    @Test
    public void test_completeExceptionally() {
        IntPromise promise = new IntPromise(reactor.eventloop());

        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference<Integer> valueRef = new AtomicReference();
        AtomicReference<Throwable> throwableRef = new AtomicReference();
        promise.then((o, throwable) -> {
            valueRef.set(o);
            throwableRef.set(throwable);
            executed.countDown();
        });

        Exception exception = new Exception();
        promise.completeExceptionally(exception);

        assertOpenEventually(executed);
        assertTrue(promise.isCompletedExceptionally());
        assertEquals(new Integer(0), valueRef.get());
        assertSame(exception, throwableRef.get());
    }


    @Test
    public void test_completeExceptionally_whenAlreadyCompleted() {
        IntPromise promise = new IntPromise(reactor.eventloop());

        promise.completeExceptionally(new Throwable());

        assertThrows(IllegalStateException.class, () -> promise.completeExceptionally(new Throwable()));
    }

    @Test
    public void test_complete() {
        IntPromise promise = new IntPromise(reactor.eventloop());

        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference valueRef = new AtomicReference();
        AtomicReference throwableRef = new AtomicReference();
        promise.then((IntBiConsumer<Throwable>) (o, throwable) -> {
            valueRef.set(o);
            throwableRef.set(throwable);
            executed.countDown();
        });

        int result = 10;
        promise.complete(result);

        assertOpenEventually(executed);
        assertSame(result, valueRef.get());
        assertNull(throwableRef.get());
    }

    @Test
    public void test_complete_whenAlreadyCompleted() {
        IntPromise promise = new IntPromise(reactor.eventloop());

        promise.complete(1);
        assertThrows(IllegalStateException.class, () -> promise.complete(2));
    }
}
