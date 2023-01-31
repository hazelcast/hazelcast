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

package com.hazelcast.internal.tpc.nio;

import com.hazelcast.internal.tpc.AsyncServerSocket;
import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.Reactor;
import com.hazelcast.internal.tpc.ReactorBuilder;

import java.nio.channels.Selector;

/**
 * Nio implementation of the {@link Reactor}.
 */
public final class NioReactor extends Reactor {

    Selector selector;

    public NioReactor() {
        this(new NioReactorBuilder());
    }

    public NioReactor(NioReactorBuilder builder) {
        super(builder);
    }

    @Override
    public AsyncServerSocket openTcpAsyncServerSocket() {
        return NioAsyncServerSocket.openTcpServerSocket(this);
    }

    @Override
    public AsyncSocket openTcpAsyncSocket() {
        return NioAsyncSocket.openTcpSocket();
    }

    @Override
    protected Eventloop createEventloop(ReactorBuilder builder) {
        NioEventloop eventloop =  new NioEventloop(this, (NioReactorBuilder) builder);
        selector = eventloop.selector;
        return eventloop;
    }

    @Override
    public void wakeup() {
        if (spin || Thread.currentThread() == eventloopThread) {
            return;
        }

        if (wakeupNeeded.get() && wakeupNeeded.compareAndSet(true, false)) {
            selector.wakeup();
        }
    }
}
