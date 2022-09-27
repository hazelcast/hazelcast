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

package com.hazelcast.tpc.engine;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.Closeable;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AsyncServerSocket implements Closeable {

    public final ConcurrentMap context = new ConcurrentHashMap();

    protected final ILogger logger = Logger.getLogger(this.getClass());
    protected final AtomicBoolean closed = new AtomicBoolean(false);

    public final SocketAddress localAddress() {
        try {
            return getLocalAddress0();
        } catch (Error e) {
            throw e;
        } catch (Exception e) {
            return null;
        }
    }

    public abstract Eventloop eventloop();

    protected abstract SocketAddress getLocalAddress0() throws Exception;

    public abstract boolean isReusePort();

    public abstract void setReusePort(boolean reusePort);

    public abstract boolean isReuseAddress();

    public abstract void reuseAddress(boolean reuseAddress);

    public abstract void receiveBufferSize(int size);

    public abstract int receiveBufferSize();

    public abstract void bind(SocketAddress socketAddress);

    public void listen(int backlog) {
    }

    public abstract void close();

    public final boolean isClosed() {
        return closed.get();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + localAddress() + "]";
    }

    public abstract int getLocalPort();
}
