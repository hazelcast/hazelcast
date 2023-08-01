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

public abstract class ReactorBuilderTest {

    public abstract Reactor.Builder newReactorBuilder();

//    @Test
//    public void test_setReactorName_whenNull() {
//        Reactor.Builder builder = newReactorContext();
//
//        assertThrows(NullPointerException.class, () -> builder.setReactorName(null));
//    }
//
//    @Test
//    public void test_setReactorName_whenAlreadyBuilt() {
//        Reactor.Builder builder = newReactorContext();
//        builder.build();
//        assertThrows(IllegalStateException.class, () -> builder.setReactorName("banana"));
//    }
//
//    @Test
//    public void test_setReactorName() {
//        Reactor.Builder builder = newReactorContext();
//        builder.setReactorName("banana");
//        Reactor reactor = builder.build();
//        assertEquals("banana", reactor.name());
//        assertEquals("banana", reactor.toString());
//    }
//
//    @Test
//    public void test_build_whenAlreadyBuilt() {
//        Reactor.Builder builder = newReactorContext();
//        builder.build();
//
//        assertThrows(IllegalStateException.class, () -> builder.build());
//    }
//
//
//    @Test
//    public void test_setStallHandler_whenNull() {
//        Reactor.Builder builder = newReactorContext();
//        assertThrows(NullPointerException.class, () -> builder.setStallHandler(null));
//    }
//
//
//    @Test
//    public void test_setStallHandler_whenAlreadyBuilt() {
//        Reactor.Builder builder = newReactorContext();
//        builder.build();
//        assertThrows(IllegalStateException.class, () -> builder.setStallHandler(new LoggingStallHandler()));
//    }
//
//
//    @Test
//    public void test_setInitFn_whenNull() {
//        Reactor.Builder builder = newReactorContext();
//        assertThrows(NullPointerException.class, () -> builder.setInitFn(null));
//    }
//
//    @Test
//    public void test_setInitFn_whenAlreadyBuilt() {
//        Reactor.Builder builder = newReactorContext();
//        builder.build();
//        assertThrows(IllegalStateException.class, () -> builder.setInitFn(mock(Consumer.class)));
//    }
//
//    @Test
//    public void test_setThreadFactory_whenNull() {
//        Reactor.Builder builder = newReactorContext();
//        assertThrows(NullPointerException.class, () -> builder.setThreadFactory(null));
//    }
//
//    @Test
//    public void test_setThreadFactory_whenAlreadyBuilt() {
//        Reactor.Builder builder = newReactorContext();
//        builder.build();
//        assertThrows(IllegalStateException.class, () -> builder.setThreadFactory(Thread::new));
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
//    public void test_setThreadName_whenAlreadyBuilt() {
//        Reactor.Builder builder = newReactorContext();
//        builder.build();
//        assertThrows(IllegalStateException.class, () -> builder.setThreadName("banana"));
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
//    public void test_setDeadlineRunQueueCapacity_whenAlreadyBuilt() {
//        Reactor.Builder builder = newReactorContext();
//        builder.build();
//        assertThrows(IllegalStateException.class, () -> builder.setDeadlineRunQueueCapacity(10));
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
//    public void test_setDefaultTaskQueueBuilder_whenAlreadyBuilt() {
//        Reactor.Builder builder = newReactorContext();
//        builder.build();
//        assertThrows(IllegalStateException.class, () -> builder.setDefaultTaskQueueContext(new TaskQueue.Context()));
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
//
//    @Test
//    public void test_setAffinity_whenAlreadyBuilt() {
//        Reactor.Builder builder = newReactorContext();
//        builder.build();
//        ThreadAffinity affinity = new ThreadAffinity("1-5");
//        assertThrows(IllegalStateException.class, () -> builder.setThreadAffinity(affinity));
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
//    public void test_setSpin_whenAlreadyBuilt() {
//        Reactor.Builder builder = newReactorContext();
//        builder.build();
//        assertThrows(IllegalStateException.class, () -> builder.setSpin(false));
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
