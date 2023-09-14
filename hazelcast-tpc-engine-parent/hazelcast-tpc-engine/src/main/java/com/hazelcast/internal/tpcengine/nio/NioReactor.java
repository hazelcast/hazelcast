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

package com.hazelcast.internal.tpcengine.nio;

import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.ReactorType;
import com.hazelcast.internal.tpcengine.net.AbstractAsyncSocket;
import com.hazelcast.internal.tpcengine.nio.NioAsyncServerSocket.AcceptRequest;

import java.nio.channels.Selector;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.Executors.newCachedThreadPool;

/**
 * Nio implementation of the {@link Reactor}.
 */
public final class NioReactor extends Reactor {

    final Selector selector;

    private NioReactor(Builder builder) {
        super(builder);
        this.selector = ((NioEventloop) eventloop()).selector;
    }

    @Override
    public NioAsyncSocket.Builder newAsyncSocketBuilder() {
        checkRunning();

        NioAsyncSocket.Builder socketBuilder = new NioAsyncSocket.Builder(null);
        socketBuilder.reactor = this;
        socketBuilder.signals = signals;
        socketBuilder.selector = selector;
        NioEventloop nioEventloop = (NioEventloop) eventloop;
        socketBuilder.networkScheduler = nioEventloop.networkScheduler();
        return socketBuilder;
    }

    @Override
    public NioAsyncSocket.Builder newAsyncSocketBuilder(
            AbstractAsyncSocket.AcceptRequest acceptRequest) {
        checkRunning();

        NioAsyncSocket.Builder socketBuilder
                = new NioAsyncSocket.Builder((AcceptRequest) acceptRequest);
        socketBuilder.reactor = this;
        socketBuilder.signals = signals;
        socketBuilder.selector = selector;
        NioEventloop nioEventloop = (NioEventloop) eventloop;
        socketBuilder.networkScheduler = nioEventloop.networkScheduler();
        return socketBuilder;
    }

    @Override
    public NioAsyncServerSocket.Builder newAsyncServerSocketBuilder() {
        checkRunning();

        NioAsyncServerSocket.Builder serverSocketBuilder = new NioAsyncServerSocket.Builder();
        serverSocketBuilder.reactor = this;
        serverSocketBuilder.selector = selector;
        return serverSocketBuilder;
    }

    @Override
    protected Eventloop newEventloop(Reactor.Builder reactorBuilder) {
        NioEventloop.Builder eventloopBuilder = new NioEventloop.Builder();
        eventloopBuilder.reactor = this;
        eventloopBuilder.reactorBuilder = reactorBuilder;
        return eventloopBuilder.build();
    }

    @Override
    public void wakeup() {
        if (idleStrategy != null || Thread.currentThread() == eventloopThread) {
            return;
        }

        if (wakeupNeeded.get() && wakeupNeeded.compareAndSet(true, false)) {
            selector.wakeup();
        }
    }

    /**
     * A {@link NioReactor} builder.
     */
    @SuppressWarnings({"checkstyle:VisibilityModifier"})
    public static class Builder extends Reactor.Builder {

        public static final Executor DEFAULT_STORAGE_EXECUTOR
                = newCachedThreadPool(new ThreadFactory() {
            private final AtomicLong counter = new AtomicLong();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("NioStorageExecutorThread-" + counter.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        });

        /**
         * The Executor used to process blocking storage requests.
         * <p>
         * This executor isn't shut down when the reactor shuts down. So it needs
         * to be managed externally.
         */
        public Executor storageExecutor;

        public Builder() {
            super(ReactorType.NIO);
        }

        @Override
        protected void conclude() {
            super.conclude();

            if (storageExecutor == null) {
                storageExecutor = DEFAULT_STORAGE_EXECUTOR;
            }
        }

        @Override
        protected NioReactor construct() {
            return new NioReactor(this);
        }
    }
}
