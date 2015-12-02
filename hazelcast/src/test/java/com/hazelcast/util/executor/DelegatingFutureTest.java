/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.executor;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DelegatingFutureTest {

    private final SerializationService serializationService
            = new DefaultSerializationServiceBuilder().build();

    @Test
    public void test_get_Object() throws ExecutionException, InterruptedException {
        Object value = "value";
        Future future = new DelegatingFuture(new FakeCompletableFuture(value), null);
        assertEquals(value, future.get());
    }

    @Test
    public void test_get_Data() throws ExecutionException, InterruptedException {
        Object value = "value";
        Data data = serializationService.toData(value);
        Future future = new DelegatingFuture(new FakeCompletableFuture(data), serializationService);
        assertEquals(value, future.get());
    }

    @Test
    public void test_get_Object_withTimeout() throws ExecutionException, InterruptedException, TimeoutException {
        Object value = "value";
        Future future = new DelegatingFuture(new FakeCompletableFuture(value), null);
        assertEquals(value, future.get(1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void test_get_Data_withTimeout() throws ExecutionException, InterruptedException, TimeoutException {
        Object value = "value";
        Data data = serializationService.toData(value);
        Future future = new DelegatingFuture(new FakeCompletableFuture(data), serializationService);
        assertEquals(value, future.get(1, TimeUnit.MILLISECONDS));
    }

    @Test(expected = ExecutionException.class)
    public void test_get_Exception() throws ExecutionException, InterruptedException {
        Throwable error = new Throwable();
        Future future = new DelegatingFuture(new FakeCompletableFuture(error), null);
        future.get();
    }

    @Test
    public void test_cancel() {
        Future future = new DelegatingFuture(new FakeCompletableFuture(null), null);
        assertFalse(future.cancel(true));
        assertFalse(future.isCancelled());
    }

    @Test
    public void test_isDone() {
        Future future = new DelegatingFuture(new FakeCompletableFuture("value"), null);
        assertTrue(future.isDone());
    }

    @Test
    public void test_andThen_Object() {
        Object value = "value";
        ICompletableFuture future = new DelegatingFuture(new FakeCompletableFuture(value), null);
        TestExecutionCallback callback = new TestExecutionCallback();
        future.andThen(callback, new CallerRunsExecutor());
        assertEquals(value, callback.value);
    }

    @Test
    public void test_andThen_Data() {
        Object value = "value";
        Data data = serializationService.toData(value);
        ICompletableFuture future = new DelegatingFuture(new FakeCompletableFuture(data), serializationService);
        TestExecutionCallback callback = new TestExecutionCallback();
        future.andThen(callback, new CallerRunsExecutor());
        assertEquals(value, callback.value);
    }

    @Test(expected = RuntimeException.class)
    public void test_andThen_Exception() {
        Throwable error = new RuntimeException();
        ICompletableFuture future = new DelegatingFuture(new FakeCompletableFuture(error), null);
        TestExecutionCallback callback = new TestExecutionCallback();
        future.andThen(callback, new CallerRunsExecutor());
    }

    private static class FakeCompletableFuture<V> implements ICompletableFuture<V> {
        private final Object value;

        private FakeCompletableFuture(Object value) {
            this.value = value;
        }

        @Override
        public void andThen(ExecutionCallback<V> callback) {
            if (value instanceof Throwable) {
                callback.onFailure((Throwable) value);
            } else {
                callback.onResponse((V) value);
            }
        }

        @Override
        public void andThen(ExecutionCallback<V> callback, Executor executor) {
            andThen(callback);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return value != null;
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            if (value instanceof ExecutionException) {
                throw (ExecutionException) value;
            }
            if (value instanceof CancellationException) {
                throw (CancellationException) value;
            }
            if (value instanceof InterruptedException) {
                throw (InterruptedException) value;
            }
            if (value instanceof Throwable) {
                throw new ExecutionException((Throwable) value);
            }
            return (V) value;
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return get();
        }
    }


    private final ExecutorService futureService = Executors.newFixedThreadPool(1);

    private final ExecutorService testService = Executors.newFixedThreadPool(2);

    @Test
    public void callFutureGetTwiceTest() {
        Future<Boolean> future = futureService.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                Thread.sleep(2000);
                return true;
            }
        });

        final DelegatingFuture<Boolean> delegatingFuture = new DelegatingFuture<Boolean>(new DummyCompletableFuture(future), null);

        Future<Boolean> resultFuture1 = testService.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    delegatingFuture.get();
                    return true;
                } catch (Exception ex) {
                    ex.printStackTrace();
                    return false;
                }
            }
        });

        Future<Boolean> resultFuture2 = testService.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                long start = 0;
                try {
                    Thread.sleep(100);
                    start = System.nanoTime();
                    delegatingFuture.get(500, TimeUnit.MILLISECONDS);
                    System.out.println("DID NOT TIME OUT WAITING FOR RESULT!");
                    return false;
                } catch (TimeoutException ex) {
                    final long delay = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    System.out.println("TIMED OUT WAITING FOR RESULT!");
                    return (delay < 550 && delay > 480);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    return false;
                } finally {
                    final long delay = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    System.out.println("Got a result after: [" + delay + "] ms");
                }
            }
        });

        try {
            Assert.assertTrue(resultFuture1.get());
            Assert.assertTrue(resultFuture2.get());
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.assertNull(ex);
        }
    }

    private class DummyCompletableFuture<V> implements ICompletableFuture<V> {

        Future future;

        public DummyCompletableFuture(final Future future) {
            this.future = future;
        }

        @Override
        public void andThen(ExecutionCallback<V> callback) {
            //DO NOTHING
        }

        @Override
        public void andThen(ExecutionCallback<V> callback, Executor executor) {
            //DO NOTHING
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return future.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            return (V) future.get();
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return (V) future.get(timeout, unit);
        }
    }
}
