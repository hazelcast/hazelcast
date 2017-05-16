/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.atomiclong;

import com.hazelcast.client.UndefinedErrorCodeException;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientAtomicLongTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance client;
    private IAtomicLong l;

    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
        l = client.getAtomicLong(randomString());
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void test() throws Exception {


        assertEquals(0, l.getAndAdd(2));
        assertEquals(2, l.get());
        l.set(5);
        assertEquals(5, l.get());
        assertEquals(8, l.addAndGet(3));
        assertFalse(l.compareAndSet(7, 4));
        assertEquals(8, l.get());
        assertTrue(l.compareAndSet(8, 4));
        assertEquals(4, l.get());
        assertEquals(3, l.decrementAndGet());
        assertEquals(3, l.getAndIncrement());
        assertEquals(4, l.getAndSet(9));
        assertEquals(10, l.incrementAndGet());

    }

    @Test
    public void testAsync() throws Exception {
        ICompletableFuture<Long> future = l.getAndAddAsync(10);
        assertEquals(0, future.get().longValue());

        ICompletableFuture<Boolean> booleanFuture = l.compareAndSetAsync(10, 42);
        assertTrue(booleanFuture.get());

        future = l.getAsync();
        assertEquals(42, future.get().longValue());

        future = l.incrementAndGetAsync();
        assertEquals(43, future.get().longValue());

        future = l.addAndGetAsync(-13);
        assertEquals(30, future.get().longValue());

        future = l.alterAndGetAsync(new AddOneFunction());
        assertEquals(31, future.get().longValue());

    }

    @Test(expected = IllegalArgumentException.class)
    public void apply_whenCalledWithNullFunction() {
        IAtomicLong ref = client.getAtomicLong("apply_whenCalledWithNullFunction");

        ref.apply(null);
    }

    @Test
    public void apply() {
        IAtomicLong ref = client.getAtomicLong("apply");

        assertEquals(new Long(1), ref.apply(new AddOneFunction()));
        assertEquals(0, ref.get());
    }

    @Test
    public void applyAsync()
            throws ExecutionException, InterruptedException {
        IAtomicLong ref = client.getAtomicLong("apply");
        ICompletableFuture<Long> future = ref.applyAsync(new AddOneFunction());
        assertEquals(new Long(1), future.get());
        assertEquals(0, ref.get());
    }

    @Test
    public void applyBooleanAsync() throws ExecutionException, InterruptedException {
        final CountDownLatch cdl = new CountDownLatch(1);
        final IAtomicLong ref = client.getAtomicLong("apply");
        ICompletableFuture<Void> incAndGetFuture = ref.setAsync(1);
        final AtomicBoolean failed = new AtomicBoolean(true);
        incAndGetFuture.andThen(new ExecutionCallback<Void>() {
            @Override
            public void onResponse(Void response) {
                ICompletableFuture<Boolean> future = ref.applyAsync(new FilterOnesFunction());
                try {
                    assertEquals(Boolean.TRUE, future.get());
                    failed.set(false);
                    cdl.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onFailure(Throwable t) {
                t.printStackTrace();
            }
        });
        if (cdl.await(15, TimeUnit.SECONDS)) {
            assertEquals(1, ref.get());
            assertFalse(failed.get());
        } else {
            fail("Timeout after 15 seconds");
        }
    }

    @Test
    public void apply_whenException() {
        IAtomicLong ref = client.getAtomicLong("apply_whenException");
        ref.set(1);
        try {
            ref.apply(new FailingFunction());
            fail();
        } catch (UndefinedErrorCodeException expected) {
            assertEquals(expected.getOriginClassName(), ExpectedRuntimeException.class.getName());
        }

        assertEquals(1, ref.get());
    }

    @Test
    public void applyAsync_whenException() {
        IAtomicLong ref = client.getAtomicLong("applyAsync_whenException");
        ref.set(1);
        try {
            ICompletableFuture<Long> future = ref.applyAsync(new FailingFunction());
            future.get();
        } catch (InterruptedException e) {
            fail();
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass(), UndefinedErrorCodeException.class);
            assertEquals(((UndefinedErrorCodeException) e.getCause()).getOriginClassName(),
                    ExpectedRuntimeException.class.getName());
        }

        assertEquals(1, ref.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void alter_whenCalledWithNullFunction() {
        IAtomicLong ref = client.getAtomicLong("alter_whenCalledWithNullFunction");

        ref.alter(null);
    }

    @Test
    public void alter_whenException() {
        IAtomicLong ref = client.getAtomicLong("alter_whenException");
        ref.set(10);

        try {
            ref.alter(new FailingFunction());
            fail();
        } catch (UndefinedErrorCodeException expected) {
            assertEquals(expected.getOriginClassName(), ExpectedRuntimeException.class.getName());
        }

        assertEquals(10, ref.get());
    }

    @Test
    public void alterAsync_whenException() {
        IAtomicLong ref = client.getAtomicLong("alterAsync_whenException");
        ref.set(10);

        try {
            ICompletableFuture<Void> future = ref.alterAsync(new FailingFunction());
            future.get();
        } catch (InterruptedException e) {
            fail();
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass(), UndefinedErrorCodeException.class);
            assertEquals(((UndefinedErrorCodeException) e.getCause()).getOriginClassName(),
                    ExpectedRuntimeException.class.getName());
        }

        assertEquals(10, ref.get());
    }

    @Test
    public void alter() {
        IAtomicLong ref = client.getAtomicLong("alter");

        ref.set(10);
        ref.alter(new AddOneFunction());
        assertEquals(11, ref.get());

    }

    @Test
    public void alterAsync()
            throws ExecutionException, InterruptedException {
        IAtomicLong ref = client.getAtomicLong("alterAsync");

        ref.set(10);
        ICompletableFuture<Void> future = ref.alterAsync(new AddOneFunction());
        future.get();
        assertEquals(11, ref.get());

    }

    @Test(expected = IllegalArgumentException.class)
    public void alterAndGet_whenCalledWithNullFunction() {
        IAtomicLong ref = client.getAtomicLong("alterAndGet_whenCalledWithNullFunction");

        ref.alterAndGet(null);
    }

    @Test
    public void alterAndGet_whenException() {
        IAtomicLong ref = client.getAtomicLong("alterAndGet_whenException");
        ref.set(10);

        try {
            ref.alterAndGet(new FailingFunction());
            fail();
        } catch (UndefinedErrorCodeException expected) {
            assertEquals(expected.getOriginClassName(), ExpectedRuntimeException.class.getName());
        }

        assertEquals(10, ref.get());
    }

    @Test
    public void alterAndGetAsync_whenException() {
        IAtomicLong ref = client.getAtomicLong("alterAndGetAsync_whenException");
        ref.set(10);

        try {
            ICompletableFuture<Long> future = ref.alterAndGetAsync(new FailingFunction());
            future.get();
        } catch (InterruptedException e) {
            fail();
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass(), UndefinedErrorCodeException.class);
            assertEquals(((UndefinedErrorCodeException) e.getCause()).getOriginClassName(),
                    ExpectedRuntimeException.class.getName());
        }

        assertEquals(10, ref.get());
    }

    @Test
    public void alterAndGet() {
        IAtomicLong ref = client.getAtomicLong("alterAndGet");

        ref.set(10);
        assertEquals(11, ref.alterAndGet(new AddOneFunction()));
        assertEquals(11, ref.get());
    }

    @Test
    public void alterAndGetAsync() throws ExecutionException, InterruptedException {
        IAtomicLong ref = client.getAtomicLong("alterAndGetAsync");

        ICompletableFuture<Void> future = ref.setAsync(10);
        future.get();
        assertEquals(11, ref.alterAndGetAsync(new AddOneFunction()).get().longValue());
        assertEquals(11, ref.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getAndAlter_whenCalledWithNullFunction() {
        IAtomicLong ref = client.getAtomicLong("getAndAlter_whenCalledWithNullFunction");

        ref.getAndAlter(null);
    }

    @Test
    public void getAndAlter_whenException() {
        IAtomicLong ref = client.getAtomicLong("getAndAlter_whenException");
        ref.set(10);

        try {
            ref.getAndAlter(new FailingFunction());
            fail();
        } catch (UndefinedErrorCodeException expected) {
            assertEquals(expected.getOriginClassName(), ExpectedRuntimeException.class.getName());
        }

        assertEquals(10, ref.get());
    }

    @Test
    public void getAndAlterAsync_whenException() {
        IAtomicLong ref = client.getAtomicLong("getAndAlterAsync_whenException");
        ref.set(10);

        try {
            ICompletableFuture<Long> future = ref.getAndAlterAsync(new FailingFunction());
            future.get();
            fail();
        } catch (InterruptedException e) {
            assertEquals(e.getCause().getClass().getName(), UndefinedErrorCodeException.class.getName());
            assertEquals(((UndefinedErrorCodeException) e.getCause()).getOriginClassName(), ExpectedRuntimeException.class.getName());
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass().getName(), UndefinedErrorCodeException.class.getName());
            assertEquals(((UndefinedErrorCodeException) e.getCause()).getOriginClassName(), ExpectedRuntimeException.class.getName());
        }

        assertEquals(10, ref.get());
    }

    @Test
    public void getAndAlter() {
        IAtomicLong ref = client.getAtomicLong("getAndAlter");

        ref.set(10);
        assertEquals(10, ref.getAndAlter(new AddOneFunction()));
        assertEquals(11, ref.get());
    }

    @Test
    public void getAndAlterAsync() throws ExecutionException, InterruptedException {
        IAtomicLong ref = client.getAtomicLong("getAndAlterAsync");

        ref.set(10);

        ICompletableFuture<Long> future = ref.getAndAlterAsync(new AddOneFunction());
        assertEquals(10, future.get().longValue());
        assertEquals(11, ref.get());
    }

    private static class AddOneFunction implements IFunction<Long, Long> {
        @Override
        public Long apply(Long input) {
            return input + 1;
        }
    }

    private static class FilterOnesFunction implements IFunction<Long, Boolean> {
        @Override
        public Boolean apply(Long input) {
            return input.equals(1L);
        }
    }


    private static class FailingFunction implements IFunction<Long, Long> {
        @Override
        public Long apply(Long input) {
            throw new ExpectedRuntimeException();
        }
    }

}
