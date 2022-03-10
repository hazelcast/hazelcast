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

package com.hazelcast.internal.tpc;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A server socket that is asynchronous. So accepting incomming connections does not block,
 * but are executed on an {@link Eventloop}.
 */
public abstract class AsyncServerSocket implements Closeable {

    public final ConcurrentMap context = new ConcurrentHashMap();

    protected final ILogger logger = Logger.getLogger(getClass());
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

    public abstract void listen(int backlog);

    @Override
    public final void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        if (logger.isInfoEnabled()) {
            logger.info("Closing  " + this);
        }

        try {
            doClose();
        } catch (Exception e) {
            logger.warning(e);
        }
    }

    protected abstract void doClose() throws IOException;

    public final boolean isClosed() {
        return closed.get();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + localAddress() + "]";
    }

    public abstract int getLocalPort();
}
