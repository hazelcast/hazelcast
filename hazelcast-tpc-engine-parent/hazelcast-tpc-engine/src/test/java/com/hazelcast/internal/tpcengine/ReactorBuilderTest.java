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

import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertTrueEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.terminateAll;
import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertEquals;

public abstract class ReactorBuilderTest {

    private final List<Reactor> reactors = new ArrayList<>();

    @After
    public void after() throws InterruptedException {
        terminateAll(reactors);
    }

    public abstract Reactor.Builder newReactorBuilder();

    @Test
    public void test_reactorName_whenNull() {
        Reactor.Builder builder = newReactorBuilder();
        builder.reactorName = null;

        int nextId = Reactor.Builder.REACTOR_ID_GENERATOR.get();

        Reactor reactor = builder.build();
        assertEquals("Reactor-" + nextId, reactor.name());
    }

    @Test
    public void test_reactorName_whenNotNull() {
        Reactor.Builder builder = newReactorBuilder();
        String name = "banana";
        builder.reactorName = name;

        Reactor reactor = builder.build();
        assertEquals(name, reactor.name());
    }

    @Test
    public void test_initFn_whenNotNull() {
        Reactor.Builder builder = newReactorBuilder();

        List<Reactor> found = synchronizedList(new ArrayList<>());
        Consumer<Reactor> initFn = found::add;
        builder.initFn = initFn;

        Reactor reactor = builder.build();
        reactors.add(reactor);
        reactor.start();

        assertTrueEventually(() -> assertEquals(Collections.singletonList(reactor), found));
    }

//    @Test
//    public void test_setStallHandler_whenNull() {
//        Reactor.Builder builder = newReactorContext();
//        assertThrows(NullPointerException.class, () -> builder.setStallHandler(null));
//    }
//
//    @Test
//    public void test_setThreadFactory_whenNull() {
//        Reactor.Builder builder = newReactorContext();
//        assertThrows(NullPointerException.class, () -> builder.setThreadFactory(null));
//    }
//
//    @Test
//    public void test_setThreadFactory() throws ExecutionException, InterruptedException {
//        Reactor.Builder builder = newReactorContext();
//        CompletableFuture<Runnable> future = new CompletableFuture<>();
//        builder.setThreadFactory(r -> {
//            Thread thread = new Thread(r);
//            future.complete(thread);
//            return thread;
//        });
//        Reactor reactor = builder.build();
//        future.join();
//        assertEquals(future.get(), reactor.eventloopThread);
//    }
//
//    @Test
//    public void test_setThreadName() {
//        Reactor.Builder builder = newReactorContext();
//
//        String name = "coolthreadname";
//        builder.setThreadName(name);
//
//        Reactor reactor = builder.build();
//        assertEquals(name, reactor.eventloopThread.getName());
//    }
//
//    @Test
//    public void test_setDeadlineRunQueueCapacity_whenZero() {
//        Reactor.Builder builder = newReactorContext();
//        assertThrows(IllegalArgumentException.class, () -> builder.setDeadlineRunQueueCapacity(0));
//    }
//
//    @Test
//    public void test_setDeadlineRunQueueCapacity_whenNegative() {
//        Reactor.Builder builder = newReactorContext();
//        assertThrows(IllegalArgumentException.class, () -> builder.setDeadlineRunQueueCapacity(-1));
//    }
//
//    @Test
//    public void test_setDefaultTaskQueueBuilder_whenNull() {
//        Reactor.Builder builder = newReactorContext();
//        assertThrows(NullPointerException.class, () -> builder.setDefaultTaskQueueContext(null));
//    }
//
//    @Test
//    public void test_setDefaultTaskQueueBuilder() {
//        Reactor.Builder builder = newReactorContext();
//        builder.setDefaultTaskQueueContext(new TaskQueue.Context()
//                .setName("banana")
//                .setGlobal(new ConcurrentLinkedQueue<>()));
//
//        Reactor reactor = builder.build();
//        assertEquals(reactor.eventloop.defaultTaskQueueHandle.queue.name, "banana");
//    }
//
//    @Test
//    public void test_setThreadAffinity_nullAffinityIsAllowed() {
//        Reactor.Builder builder = newReactorContext();
//        builder.setThreadAffinity(null);
//
//        assertNull(builder.threadAffinity);
//    }
//
//    @Test
//    public void test_setThreadAffinity() {
//        Reactor.Builder builder = newReactorContext();
//        ThreadAffinity affinity = new ThreadAffinity("1-5");
//        builder.setThreadAffinity(affinity);
//
//        assertSame(affinity, builder.threadAffinity);
//    }
//
//    @Test
//    public void test_setSpin() {
//        Reactor.Builder builder = newReactorContext();
//        builder.setSpin(true);
//
//        assertTrue(builder.spin);
//    }
}
