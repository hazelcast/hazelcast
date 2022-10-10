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

import com.hazelcast.internal.tpc.AcceptRequest;
import com.hazelcast.internal.tpc.AsyncServerSocket;
import com.hazelcast.internal.tpc.AsyncServerSocketBuilder;
import com.hazelcast.internal.tpc.Option;

import java.util.function.Consumer;

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;

public class IOUringAsyncServerSocketBuilder implements AsyncServerSocketBuilder {

    final IOUringReactor reactor;
    final NativeSocket nativeSocket;
    final IOUringAsyncServerSocketOptions options;
    Consumer<AcceptRequest> acceptConsumer;
    private boolean build;

    IOUringAsyncServerSocketBuilder(IOUringReactor reactor) {
        this.reactor = reactor;
        this.nativeSocket = NativeSocket.openTcpIpv4Socket();
        nativeSocket.setBlocking(true);
        this.options = new IOUringAsyncServerSocketOptions(nativeSocket);
    }

    @Override
    public IOUringAsyncServerSocketBuilder setAcceptConsumer(Consumer<AcceptRequest> acceptConsumer) {
        verifyNotBuild();

        this.acceptConsumer = checkNotNull(acceptConsumer, "acceptConsumer");
        return this;
    }

    @Override
    public <T> IOUringAsyncServerSocketBuilder set(Option<T> option, T value) {
        verifyNotBuild();

        options.set(option, value);
        return this;
    }

    @Override
    public AsyncServerSocket build() {
        verifyNotBuild();

        if (acceptConsumer == null) {
            throw new IllegalStateException("acceptConsumer not configured.");
        }

        build = true;
        return new IOUringAsyncServerSocket(this);
    }

    private void verifyNotBuild() {
        if (build) {
            throw new IllegalStateException("Can't call build twice on the same AsyncServerSocketBuilder");
        }
    }
}
