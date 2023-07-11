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

package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.util.ThreadAffinity;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public abstract class ReactorBuilderTest {

    public abstract ReactorBuilder newBuilder();

    @Test
    public void test_setReactorNameSupplier_whenNull() {
        ReactorBuilder builder = newBuilder();
        assertThrows(NullPointerException.class, () -> builder.setReactorNameSupplier(null));
    }

    @Test
    public void test_setReactorNameSupplier() {
        ReactorBuilder builder = newBuilder();
        builder.setReactorNameSupplier(new Supplier<>() {
            private AtomicInteger idGenerator = new AtomicInteger();

            @Override
            public String get() {
                return "banana-" + idGenerator.incrementAndGet();
            }
        });
        Reactor reactor1 = builder.build();
        assertEquals("banana-1", reactor1.name());
        assertEquals("banana-1", reactor1.toString());
    }

    @Test
    public void test_setStallHandler_whenNull() {
        ReactorBuilder builder = newBuilder();
        assertThrows(NullPointerException.class, () -> builder.setStallHandler(null));
    }

    @Test
    public void test_setInitCommand_whenNull() {
        ReactorBuilder builder = newBuilder();
        assertThrows(NullPointerException.class, () -> builder.setInitCommand(null));
    }

    @Test
    public void test_setThreadFactory_whenNull() {
        ReactorBuilder builder = newBuilder();
        assertThrows(NullPointerException.class, () -> builder.setThreadFactory(null));
    }

    @Test
    public void test_setThreadFactory() throws ExecutionException, InterruptedException {
        ReactorBuilder builder = newBuilder();
        CompletableFuture<Runnable> future = new CompletableFuture<>();
        builder.setThreadFactory(r -> {
            Thread thread = new Thread(r);
            future.complete(thread);
            return thread;
        });
        Reactor reactor = builder.build();
        future.join();
        assertEquals(future.get(), reactor.eventloopThread);
    }

    @Test
    public void test_setThreadNameSupplier() {
        ReactorBuilder builder = newBuilder();

        AtomicInteger id = new AtomicInteger();
        builder.setThreadNameSupplier(() -> "thethread" + id.incrementAndGet());

        Reactor reactor1 = builder.build();
        Reactor reactor2 = builder.build();
        assertEquals("thethread1", reactor1.eventloopThread.getName());
        assertEquals("thethread2", reactor2.eventloopThread.getName());
    }

    @Test
    public void test_setScheduledTaskQueueCapacity_whenZero() {
        ReactorBuilder builder = newBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setDeadlineRunQueueCapacity(0));
    }

    @Test
    public void test_setScheduledTaskQueueCapacity_whenNegative() {
        ReactorBuilder builder = newBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setDeadlineRunQueueCapacity(-1));
    }

    // primordial task queue builder

    @Test
    public void test_setThreadAffinity_nullAffinityIsAllowed() {
        ReactorBuilder builder = newBuilder();
        builder.setThreadAffinity(null);

        assertNull(builder.threadAffinity);
    }

    @Test
    public void test_setThreadAffinity() {
        ReactorBuilder builder = newBuilder();
        ThreadAffinity affinity = new ThreadAffinity("1-5");
        builder.setThreadAffinity(affinity);

        assertSame(affinity, builder.threadAffinity);
    }

    @Test
    public void test_setSpin() {
        ReactorBuilder builder = newBuilder();
        builder.setSpin(true);

        assertTrue(builder.spin);
    }
}
