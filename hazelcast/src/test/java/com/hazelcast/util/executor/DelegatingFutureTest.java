/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DelegatingFutureTest {

    private final SerializationService serializationService
            = new DefaultSerializationServiceBuilder().build();

    @Test
    public void test_get_Object() throws Exception {
        Object value = "value";
        Future future = new DelegatingFuture(new FakeCompletableFuture(value), null);
        assertEquals(value, future.get());
    }

    @Test
    public void test_get_withDefault() throws Exception {
        String defaultValue = "defaultValue";
        Future<String> future = new DelegatingFuture<String>(new FakeCompletableFuture(null), serializationService, defaultValue);
        assertSame(defaultValue, future.get());
    }

    @Test
    public void test_get_Data() throws Exception {
        Object value = "value";
        Data data = serializationService.toData(value);
        Future future = new DelegatingFuture(new FakeCompletableFuture(data), serializationService);
        assertEquals(value, future.get());
    }

    @Test
    public void test_get_whenData_andMultipleTimesInvoked_thenSameInstanceReturned() throws Exception {
        Object value = "value";
        Data data = serializationService.toData(value);
        Future future = new DelegatingFuture(new FakeCompletableFuture(data), serializationService);

        Object result1 = future.get();
        Object result2 = future.get();
        assertSame(result1, result2);
    }

    @Test
    public void test_get_Object_withTimeout() throws Exception {
        Object value = "value";
        Future future = new DelegatingFuture(new FakeCompletableFuture(value), null);
        assertEquals(value, future.get(1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void test_get_Data_withTimeout() throws Exception {
        Object value = "value";
        Data data = serializationService.toData(value);
        Future future = new DelegatingFuture(new FakeCompletableFuture(data), serializationService);
        assertEquals(value, future.get(1, TimeUnit.MILLISECONDS));
    }

    @Test(expected = ExecutionException.class)
    public void test_get_Exception() throws Exception {
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

    private static class FakeCompletableFuture<V> implements InternalCompletableFuture<V> {
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

        @Override
        public V join() {
            try {
                return get();
            } catch (Throwable throwable) {
                throw rethrow(throwable);
            }
        }

        @Override
        public boolean complete(Object value) {
            return false;
        }
    }
}
