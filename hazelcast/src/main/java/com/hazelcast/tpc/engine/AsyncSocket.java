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

import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.tpc.engine.frame.Frame;

import java.io.Closeable;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;

public abstract class AsyncSocket implements Closeable {
    protected final ILogger logger = Logger.getLogger(getClass());
    protected final AtomicBoolean closed = new AtomicBoolean();

    protected volatile SocketAddress remoteAddress;
    protected volatile SocketAddress localAddress;

    public final SwCounter framesWritten = newSwCounter();

    public final SwCounter bytesRead = newSwCounter();

    public final SwCounter bytesWritten = newSwCounter();

    public final SwCounter framesRead = newSwCounter();

    public final SwCounter handleWriteCnt = newSwCounter();

    public final SwCounter readEvents = newSwCounter();

    public abstract Eventloop getEventloop();

    public final SocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public final SocketAddress getLocalAddress() {
        return localAddress;
    }

    public abstract void setSoLinger(int soLinger);

    public abstract int getSoLinger();

    public abstract void setKeepAlive(boolean keepAlive);

    public abstract boolean isKeepAlive();

    public abstract void setTcpNoDelay(boolean tcpNoDelay);

    public abstract boolean isTcpNoDelay();

    public abstract void setReceiveBufferSize(int size);

    public abstract int getReceiveBufferSize();

    public abstract void setSendBufferSize(int size);

    public abstract int getSendBufferSize();

    public abstract void setReadHandler(ReadHandler readHandler);

    public abstract void activate(Eventloop eventloop);

    public abstract void flush();

    public abstract boolean write(Frame frame);

    public abstract boolean writeAll(Collection<Frame> frames);

    public abstract boolean writeAndFlush(Frame frame);

    /**
     * Should only be called from within the Eventloop.
     */
    public abstract boolean unsafeWriteAndFlush(Frame frame);

    public abstract CompletableFuture<AsyncSocket> connect(SocketAddress address);

    public abstract void close();

    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + localAddress + "->" + remoteAddress + "]";
    }
}
