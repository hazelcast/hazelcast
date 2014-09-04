/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

public interface SocketChannelWrapper extends Closeable {

    boolean isBlocking();

    Socket socket();

    boolean isConnected();

    boolean connect(java.net.SocketAddress socketAddress) throws IOException;

    SelectionKey keyFor(Selector selector);

    SelectionKey register(Selector selector, int ops, Object attachment) throws ClosedChannelException;

    int read(ByteBuffer byteBuffer) throws IOException;

    int write(ByteBuffer byteBuffer) throws IOException;

    SelectableChannel configureBlocking(boolean b) throws IOException;

    boolean isOpen();

    /**
     * Closes inbound.
     *
     * <p>Not thread safe. Should be called in channel reader thread.</p>
     * @throws IOException
     */
    void closeInbound() throws IOException;

    /**
     * Closes outbound.
     *
     * <p>Not thread safe. Should be called in channel writer thread.</p>
     * @throws IOException
     */
    void closeOutbound() throws IOException;

    /**
     * Closes socket channel.
     *
     * @throws IOException
     */
    void close() throws IOException;
}
