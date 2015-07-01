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

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class CompletedFutureTest {

    private final SerializationService serializationService
            = new DefaultSerializationServiceBuilder().build();

    @Test
    public void test_get_Object() throws ExecutionException, InterruptedException {
        Object value = "value";
        Future future = new CompletedFuture(null, value, null);
        assertEquals(value, future.get());
    }

    @Test
    public void test_get_Data() throws ExecutionException, InterruptedException {
        Object value = "value";
        Data data = serializationService.toData(value);
        Future future = new CompletedFuture(serializationService, data, null);
        assertEquals(value, future.get());
    }

    @Test
    public void test_get_Object_withTimeout() throws ExecutionException, InterruptedException, TimeoutException {
        Object value = "value";
        Future future = new CompletedFuture(null, value, null);
        assertEquals(value, future.get(1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void test_get_Data_withTimeout() throws ExecutionException, InterruptedException, TimeoutException {
        Object value = "value";
        Data data = serializationService.toData(value);
        Future future = new CompletedFuture(serializationService, data, null);
        assertEquals(value, future.get(1, TimeUnit.MILLISECONDS));
    }

    @Test(expected = ExecutionException.class)
    public void test_get_Exception() throws ExecutionException, InterruptedException {
        Throwable error = new Throwable();
        Future future = new CompletedFuture(null, error, null);
        future.get();
    }

    @Test(expected = ExecutionException.class)
    public void test_get_Exception_Data() throws ExecutionException, InterruptedException {
        Throwable error = new Throwable();
        Data data = serializationService.toData(error);
        Future future = new CompletedFuture(serializationService, data, null);
        future.get();
    }

    @Test
    public void test_cancel() {
        Future future = new CompletedFuture(null, null, null);
        assertFalse(future.cancel(true));
        assertFalse(future.isCancelled());
    }

    @Test
    public void test_isDone() {
        Future future = new CompletedFuture(null, null, null);
        assertTrue(future.isDone());
    }

    @Test
    public void test_andThen_Object() {
        Object value = "value";
        ICompletableFuture future = new CompletedFuture(null, value, null);
        TestExecutionCallback callback = new TestExecutionCallback();
        future.andThen(callback, new CallerRunsExecutor());
        assertEquals(value, callback.value);
    }

    @Test
    public void test_andThen_Data() {
        Object value = "value";
        Data data = serializationService.toData(value);
        ICompletableFuture future = new CompletedFuture(serializationService, data, null);
        TestExecutionCallback callback = new TestExecutionCallback();
        future.andThen(callback, new CallerRunsExecutor());
        assertEquals(value, callback.value);
    }

    @Test(expected = RuntimeException.class)
    public void test_andThen_Exception() {
        Throwable error = new RuntimeException();
        ICompletableFuture future = new CompletedFuture(null, error, null);
        TestExecutionCallback callback = new TestExecutionCallback();
        future.andThen(callback, new CallerRunsExecutor());
    }

    @Test(expected = RuntimeException.class)
    public void test_andThen_Exception_Data() {
        Throwable error = new RuntimeException();
        Data data = serializationService.toData(error);
        ICompletableFuture future = new CompletedFuture(serializationService, data, null);
        TestExecutionCallback callback = new TestExecutionCallback();
        future.andThen(callback, new CallerRunsExecutor());
    }

}
