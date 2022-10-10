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

package com.hazelcast.internal.tpc.iouring;

import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.AsyncSocketBuilder;
import com.hazelcast.internal.tpc.Option;
import com.hazelcast.internal.tpc.ReadHandler;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.tpc.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;

public class IOUringAsyncSocketBuilder implements AsyncSocketBuilder {

    final IOUringReactor reactor;
    final NativeSocket nativeSocket;
    final IOUringAcceptRequest acceptRequest;
    final boolean clientSide;
    ReadHandler readHandler;
    IOUringAsyncSocketOptions options;
    private boolean build;

    IOUringAsyncSocketBuilder(IOUringReactor reactor, IOUringAcceptRequest acceptRequest) {
        this.reactor = reactor;
        this.acceptRequest = acceptRequest;
        if (acceptRequest == null) {
            this.nativeSocket = NativeSocket.openTcpIpv4Socket();
            this.clientSide = true;
        } else {
            this.nativeSocket = acceptRequest.nativeSocket;
            this.clientSide = false;
        }
        this.options = new IOUringAsyncSocketOptions(nativeSocket);
    }

    @Override
    public <T> IOUringAsyncSocketBuilder set(Option<T> option, T value) {
        verifyNotBuild();

        options.set(option, value);
        return this;
    }

    @Override
    public final IOUringAsyncSocketBuilder setReadHandler(ReadHandler readHandler) {
        verifyNotBuild();

        this.readHandler = checkNotNull(readHandler);
        return this;
    }

    @Override
    public AsyncSocket build() {
        verifyNotBuild();

        build = true;

        if (readHandler == null) {
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
