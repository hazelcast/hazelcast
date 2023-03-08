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
import com.hazelcast.internal.tpcengine.net.AcceptRequest;
import com.hazelcast.internal.tpcengine.net.AsyncServerSocket;
import com.hazelcast.internal.tpcengine.net.AsyncServerSocketBuilder;

import java.util.function.Consumer;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

public class IOUringAsyncServerSocketBuilder implements AsyncServerSocketBuilder {

    final IOUringReactor reactor;
    final LinuxSocket nativeSocket;
    final IOUringAsyncServerSocketOptions options;
    Consumer<AcceptRequest> acceptConsumer;
    private boolean build;

    IOUringAsyncServerSocketBuilder(IOUringReactor reactor) {
        this.reactor = reactor;
        this.nativeSocket = LinuxSocket.openTcpIpv4Socket();
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
    public <T> boolean setIfSupported(Option<T> option, T value) {
        verifyNotBuild();

        return options.setIfSupported(option, value);
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
