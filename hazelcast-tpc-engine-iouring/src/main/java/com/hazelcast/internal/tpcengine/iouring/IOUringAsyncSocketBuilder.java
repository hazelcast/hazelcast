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

package com.hazelcast.internal.tpcengine.iouring;

import com.hazelcast.internal.tpcengine.Option;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocketBuilder;
import com.hazelcast.internal.tpcengine.net.AsyncSocketReader;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

public class IOUringAsyncSocketBuilder implements AsyncSocketBuilder {

    final IOUringReactor reactor;
    final LinuxSocket nativeSocket;
    final IOUringAcceptRequest acceptRequest;
    final boolean clientSide;
    AsyncSocketReader reader;
    IOUringAsyncSocketOptions options;
    private boolean build;

    IOUringAsyncSocketBuilder(IOUringReactor reactor, IOUringAcceptRequest acceptRequest) {
        this.reactor = reactor;
        this.acceptRequest = acceptRequest;
        if (acceptRequest == null) {
            this.nativeSocket = LinuxSocket.openTcpIpv4Socket();
            this.clientSide = true;
        } else {
            this.nativeSocket = acceptRequest.nativeSocket;
            this.clientSide = false;
        }
        this.options = new IOUringAsyncSocketOptions(nativeSocket);
    }

    @Override
    public <T> boolean setIfSupported(Option<T> option, T value) {
        verifyNotBuild();

        return options.setIfSupported(option, value);
    }

    @Override
    public final IOUringAsyncSocketBuilder setReader(AsyncSocketReader reader) {
        verifyNotBuild();

        this.reader = checkNotNull(reader);
        return this;
    }

    @Override
    public AsyncSocket build() {
        verifyNotBuild();

        build = true;

        if (reader == null) {
            throw new IllegalStateException("readHandler is not configured.");
        }

        if (Thread.currentThread() == reactor.eventloopThread()) {
            return new IOUringAsyncSocket(this);
        } else {
            CompletableFuture<IOUringAsyncSocket> future = new CompletableFuture<>();
            reactor.execute(() -> {
                try {
                    IOUringAsyncSocket asyncSocket = new IOUringAsyncSocket(IOUringAsyncSocketBuilder.this);
                    future.complete(asyncSocket);
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                    throw sneakyThrow(e);
                }
            });

            return future.join();
        }
    }

    private void verifyNotBuild() {
        if (build) {
            throw new IllegalStateException("Can't call build twice on the same AsyncSocketBuilder");
        }
    }
}
