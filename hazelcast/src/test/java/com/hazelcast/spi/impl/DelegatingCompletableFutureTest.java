/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.impl.InternalCompletableFuture.completedExceptionally;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DelegatingCompletableFutureTest {

    private final InternalSerializationService serializationService
            = new DefaultSerializationServiceBuilder().build();

    @Test
    public void test_get_Object() throws Exception {
        Object value = "value";
        Future future = new DelegatingCompletableFuture(null, newCompletedFuture(value));
        assertEquals(value, future.get());
    }

    @Test
    public void test_get_withDefault() throws Exception {
        String defaultValue = "defaultValue";
        Future<String> future = new DelegatingCompletableFuture(serializationService, newCompletedFuture(null), defaultValue);
        assertSame(defaultValue, future.get());
    }

    @Test
    public void test_get_Data() throws Exception {
        Object value = "value";
        Data data = serializationService.toData(value);
        Future future = new DelegatingCompletableFuture(serializationService, newCompletedFuture(data));
        assertEquals(value, future.get());
    }

    @Test
    public void test_get_whenData_andMultipleTimesInvoked_thenSameInstanceReturned() throws Exception {
        Object value = "value";
        Data data = serializationService.toData(value);
        Future future = new DelegatingCompletableFuture(serializationService, newCompletedFuture(data));

        Object result1 = future.get();
        Object result2 = future.get();
        assertSame(result1, result2);
    }

    @Test
    public void test_get_Object_withTimeout() throws Exception {
        Object value = "value";
        Future future = new DelegatingCompletableFuture(null, newCompletedFuture(value));
        assertEquals(value, future.get(1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void test_get_Data_withTimeout() throws Exception {
        Object value = "value";
        Data data = serializationService.toData(value);
        Future future = new DelegatingCompletableFuture(serializationService, newCompletedFuture(data));
        assertEquals(value, future.get(1, TimeUnit.MILLISECONDS));
    }

    @Test(expected = ExecutionException.class)
    public void test_get_Exception() throws Exception {
        Throwable error = new Throwable();
        Future future = new DelegatingCompletableFuture(null, completedExceptionally(error));
        future.get();
    }

    @Test
    public void test_cancel() {
        Future future = new DelegatingCompletableFuture(null, newCompletedFuture(null));
        assertFalse(future.cancel(true));
        assertFalse(future.isCancelled());
    }

    @Test
    public void test_isDone() {
        Future future = new DelegatingCompletableFuture(null, newCompletedFuture("value"));
        assertTrue(future.isDone());
    }
}
