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

package com.hazelcast.spi.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.impl.InternalCompletableFuture.completedExceptionally;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
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
        Future future = new DelegatingCompletableFuture(serializationService, newCompletedFuture(value));
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
        Future future = new DelegatingCompletableFuture(serializationService, newCompletedFuture(value));
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
        Future future = new DelegatingCompletableFuture(serializationService, completedExceptionally(error));
        future.get();
    }

    @Test
    public void test_cancel() {
        Future future = new DelegatingCompletableFuture(serializationService, newCompletedFuture(null));
        assertFalse(future.cancel(true));
        assertFalse(future.isCancelled());
    }

    @Test
    public void test_isDone() {
        Future future = new DelegatingCompletableFuture(serializationService, newCompletedFuture("value"));
        assertTrue(future.isDone());
    }

    @Test
    public void test_actionsTrigger_whenAlreadyCompletedFuture() {
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<String> future =
                new DelegatingCompletableFuture(serializationService, newCompletedFuture("value"));
        future.thenRun(latch::countDown);
        assertOpenEventually(latch);
    }

    @Test
    public void testInteroperability1() {
        // given f1 (JDK CompletableFuture), is completed asynchronously after 3 seconds
        //       f2 (DelegatingCompletableFuture wrapping f1) returned from thenCompose
        // then  f2 is completed eventually
        CompletableFuture f1 = CompletableFuture.runAsync(() -> sleepSeconds(2));
        CompletableFuture f2 = CompletableFuture.completedFuture(null).thenCompose(v -> {
            return new DelegatingCompletableFuture<>(serializationService, f1);
        });

        assertTrueEventually(() -> assertTrue(f2.isDone() && !f2.isCompletedExceptionally()));
    }

    @Test
    public void testInteroperability2() {
        // given f1 (JDK CompletableFuture), is already completed
        //       f2 (DelegatingCompletableFuture wrapping f1) returned from thenCompose
        // then  f2 is completed eventually
        CompletableFuture f1 = CompletableFuture.completedFuture(null);
        CompletableFuture f2 = CompletableFuture.completedFuture(null).thenCompose(v ->
                new DelegatingCompletableFuture<>(serializationService, f1)
        );

        assertTrueEventually(() -> assertTrue(f2.isDone() && !f2.isCompletedExceptionally()));
    }

    @Test
    public void testInteroperability3() {
        // given f1 (DelegatingCompletableFuture wrapping completed future)
        //       f2 (JDK CompletableFuture) returned from thenCompose
        // then  f2 is completed eventually
        CompletableFuture f1 = new DelegatingCompletableFuture<>(serializationService, completedFuture(null));
        CompletableFuture f2 = f1.thenCompose(v -> CompletableFuture.runAsync(() -> sleepSeconds(2)));

        assertTrueEventually(() -> assertTrue(f2.isDone() && !f2.isCompletedExceptionally()));
    }

    @Test
    public void testInteroperability_withValueDeserializationInFunction() {
        // given f1 (DelegatingCompletableFuture wrapping completed future with Data as value)
        //       f2 (JDK CompletableFuture) returned from thenCompose
        // then  argument in compose function is deserialized
        //       f2 is completed eventually
        String value = "test";
        Data valueData = serializationService.toData(value);
        CompletableFuture f1 = new DelegatingCompletableFuture<>(serializationService, completedFuture(valueData));
        CompletableFuture f2 = f1.thenCompose(v -> {
            assertEquals(value, v);
            return CompletableFuture.runAsync(() -> sleepSeconds(2));
        });

        assertTrueEventually(() -> assertTrue(f2.isDone() && !f2.isCompletedExceptionally()));
        assertEquals(value, f1.join());
    }

    @Test
    public void testInteroperability_withValueDeserializationInCompletedReturnedFuture() {
        // given f1 (DelegatingCompletableFuture wrapping completed future)
        //       f2 (JDK CompletableFuture) returned from thenCompose
        // then  f2 is completed eventually
        //       completion value of f2 is deserialized
        String value = "test";
        Data valueData = serializationService.toData(value);
        CompletableFuture f1 = completedFuture(null);
        CompletableFuture<String> f2 = f1.thenCompose(v ->
            new DelegatingCompletableFuture<String>(serializationService, completedFuture(valueData))
        );

        assertTrueEventually(() -> assertTrue(f2.isDone() && !f2.isCompletedExceptionally()));
        assertEquals(value, f2.join());
    }

    @Test
    public void testInteroperability_withValueDeserializationInReturnedFuture() {
        // given f1 (DelegatingCompletableFuture wrapping completed future)
        //       f2 (JDK CompletableFuture) returned from thenCompose
        // then  f2 is completed eventually
        //       completion value of f2 is deserialized
        String value = "test";
        Data valueData = serializationService.toData(value);
        CompletableFuture f1 = completedFuture(null);
        CompletableFuture<String> f2 = f1.thenCompose(v ->
            new DelegatingCompletableFuture<String>(serializationService, supplyAsync(() -> {
                sleepSeconds(2);
                return valueData;
            }))
        );

        assertTrueEventually(() -> assertTrue(f2.isDone() && !f2.isCompletedExceptionally()));
        assertEquals(value, f2.join());
    }
}
