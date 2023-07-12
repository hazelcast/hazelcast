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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public abstract class ReactorBuilderTest {

    public abstract ReactorBuilder newBuilder();

    @Test
    public void test_setReactorName_whenNull() {
        ReactorBuilder builder = newBuilder();
        assertThrows(NullPointerException.class, () -> builder.setReactorName(null));
    }

    @Test
    public void test_setReactorName_whenAlreadyBuilt() {
        ReactorBuilder builder = newBuilder();
        builder.build();
        assertThrows(IllegalStateException.class, () -> builder.setReactorName("banana"));
    }

    @Test
    public void test_setReactorName() {
        ReactorBuilder builder = newBuilder();
        builder.setReactorName("banana");
        Reactor reactor = builder.build();
        assertEquals("banana", reactor.name());
        assertEquals("banana", reactor.toString());
    }

    @Test
    public void test_build_whenAlreadyBuilt() {
        ReactorBuilder builder = newBuilder();
        builder.build();

        assertThrows(IllegalStateException.class, () -> builder.build());
    }


    @Test
    public void test_setStallHandler_whenNull() {
        ReactorBuilder builder = newBuilder();
        assertThrows(NullPointerException.class, () -> builder.setStallHandler(null));
    }


    @Test
    public void test_setStallHandler_whenAlreadyBuilt() {
        ReactorBuilder builder = newBuilder();
        builder.build();
        assertThrows(IllegalStateException.class, () -> builder.setStallHandler(new LoggingStallHandler()));
    }


    @Test
    public void test_setInitFn_whenNull() {
        ReactorBuilder builder = newBuilder();
        assertThrows(NullPointerException.class, () -> builder.setInitFn(null));
    }

    @Test
    public void test_setInitFn_whenAlreadyBuilt() {
        ReactorBuilder builder = newBuilder();
        builder.build();
        assertThrows(IllegalStateException.class, () -> builder.setInitFn(mock(Consumer.class)));
    }

    @Test
    public void test_setThreadFactory_whenNull() {
        ReactorBuilder builder = newBuilder();
        assertThrows(NullPointerException.class, () -> builder.setThreadFactory(null));
    }

    @Test
    public void test_setThreadFactory_whenAlreadyBuilt() {
        ReactorBuilder builder = newBuilder();
        builder.build();
        assertThrows(IllegalStateException.class, () -> builder.setThreadFactory(Thread::new));
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
    public void test_setThreadName_whenAlreadyBuilt() {
        ReactorBuilder builder = newBuilder();
        builder.build();
        assertThrows(IllegalStateException.class, () -> builder.setThreadName("banana"));
    }

    @Test
    public void test_setThreadName() {
        ReactorBuilder builder = newBuilder();

        String name = "coolthreadname";
        builder.setThreadName(name);

        Reactor reactor = builder.build();
        assertEquals(name, reactor.eventloopThread.getName());
    }

    @Test
    public void test_setDeadlineRunQueueCapacity_whenAlreadyBuilt() {
        ReactorBuilder builder = newBuilder();
        builder.build();
        assertThrows(IllegalStateException.class, () -> builder.setDeadlineRunQueueCapacity(10));
    }

    @Test
    public void test_setDeadlineRunQueueCapacity_whenZero() {
        ReactorBuilder builder = newBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setDeadlineRunQueueCapacity(0));
    }

    @Test
    public void test_setDeadlineRunQueueCapacity_whenNegative() {
        ReactorBuilder builder = newBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setDeadlineRunQueueCapacity(-1));
    }

    @Test
    public void test_setDefaultTaskQueueBuilder_whenNull() {
        ReactorBuilder builder = newBuilder();
        assertThrows(NullPointerException.class, () -> builder.setDefaultTaskQueueBuilder(null));
    }

    @Test
    public void test_setDefaultTaskQueueBuilder_whenAlreadyBuilt() {
        ReactorBuilder builder = newBuilder();
        builder.build();
        assertThrows(IllegalStateException.class, () -> builder.setDefaultTaskQueueBuilder(new TaskQueueBuilder()));
    }

    @Test
    public void test_setDefaultTaskQueueBuilder() {
        ReactorBuilder builder = newBuilder();
        builder.setDefaultTaskQueueBuilder(new TaskQueueBuilder()
                .setName("banana")
                .setGlobal(new ConcurrentLinkedQueue<>()));

        Reactor reactor = builder.build();
        assertEquals(reactor.eventloop.defaultTaskQueueHandle.queue.name, "banana");
    }

    @Test
    public void test_setThreadAffinity_nullAffinityIsAllowed() {
        ReactorBuilder builder = newBuilder();
        builder.setThreadAffinity(null);

        assertNull(builder.threadAffinity);
    }


    @Test
    public void test_setAffinity_whenAlreadyBuilt() {
        ReactorBuilder builder = newBuilder();
        builder.build();
        ThreadAffinity affinity = new ThreadAffinity("1-5");
        assertThrows(IllegalStateException.class, () -> builder.setThreadAffinity(affinity));
    }

    @Test
    public void test_setThreadAffinity() {
        ReactorBuilder builder = newBuilder();
        ThreadAffinity affinity = new ThreadAffinity("1-5");
        builder.setThreadAffinity(affinity);

        assertSame(affinity, builder.threadAffinity);
    }

    @Test
    public void test_setSpin_whenAlreadyBuilt() {
        ReactorBuilder builder = newBuilder();
        builder.build();
        assertThrows(IllegalStateException.class, () -> builder.setSpin(false));
    }

    @Test
    public void test_setSpin() {
        ReactorBuilder builder = newBuilder();
        builder.setSpin(true);

        assertTrue(builder.spin);
    }
}
