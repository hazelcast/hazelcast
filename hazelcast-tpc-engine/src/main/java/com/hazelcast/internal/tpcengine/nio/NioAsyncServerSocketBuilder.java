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

import com.hazelcast.internal.tpcengine.net.AcceptRequest;
import com.hazelcast.internal.tpcengine.net.AsyncServerSocket;
import com.hazelcast.internal.tpcengine.net.AsyncServerSocketBuilder;
import com.hazelcast.internal.tpcengine.Option;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * A {@link AsyncServerSocketBuilder} specific to the {@link AsyncServerSocket}.
 */
public class NioAsyncServerSocketBuilder implements AsyncServerSocketBuilder {

    final NioReactor reactor;
    final ServerSocketChannel serverSocketChannel;
    final NioAsyncServerSocketOptions options;
    Consumer<AcceptRequest> acceptConsumer;
    private boolean built;

    NioAsyncServerSocketBuilder(NioReactor reactor) {
        this.reactor = reactor;
        try {
            this.serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            this.options = new NioAsyncServerSocketOptions(serverSocketChannel);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public NioAsyncServerSocketBuilder setAcceptConsumer(Consumer<AcceptRequest> acceptConsumer) {
        verifyNotBuilt();

        this.acceptConsumer = checkNotNull(acceptConsumer, "acceptConsumer");
        return this;
    }

    @Override
    public <T> boolean setIfSupported(Option<T> option, T value) {
        verifyNotBuilt();

        return options.set(option, value);
    }

    @SuppressWarnings("java:S1181")
    @Override
    public AsyncServerSocket build() {
        verifyNotBuilt();

        if (acceptConsumer == null) {
            throw new IllegalStateException("acceptConsumer not configured.");
        }

        built = true;

        if (Thread.currentThread() == reactor.eventloopThread()) {
            return new NioAsyncServerSocket(this);
        } else {
            CompletableFuture<NioAsyncServerSocket> future = new CompletableFuture<>();
            reactor.execute(() -> {
                try {
                    NioAsyncServerSocket asyncServerSocket = new NioAsyncServerSocket(this);
                    future.complete(asyncServerSocket);
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                    throw sneakyThrow(e);
                }
            });

            return future.join();
        }
    }

    private void verifyNotBuilt() {
        if (built) {
            throw new IllegalStateException("Can't call build twice on the same AsyncServerSocketBuilder");
        }
    }
}
