/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking;

import com.hazelcast.nio.OutboundFrame;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentMap;

/**
 * Responsible for transmitting or receiving data.
 *
 * It could be seen as a wrapper around e.g. a {@link SocketChannel}; but a Channel isn't bound to a particular protocol
 * like TCP/IP.
 *
 * Channel and {@link EventLoopGroup} are very closely related. For example a
 * {@link com.hazelcast.internal.networking.nio.NioEventLoopGroup} requires a {@link NioChannel}.
 */
public interface Channel extends Closeable {

    /**
     * Returns the attribute map.
     *
     * Attribute map can be used to store data into a socket. For example to find the Connection for a Channel, one can
     * store the Connection in this channel using some well known key.
     *
     * @return the attribute map.
     */
    ConcurrentMap attributeMap();

    /**
     * @see java.nio.channels.SocketChannel#socket()
     *
     * This method will be removed from the interface. Only an explicit cast to NioChannel will expose the Socket.
     */
    Socket socket();

    /**
     * @return the remote address. Returned value could be null.
     */
    SocketAddress getRemoteSocketAddress();

    /**
     * @return the local address. Returned value could be null
     */
    SocketAddress getLocalSocketAddress();

    /**
     * This method will be removed from the interface. Only an explicit cast to NioChannel will expose the SocketChannel.
     */
    boolean isConnected();

    /**
     * Will be removed in the future; only reason this method exists is for TLS.
     *
     * @see java.nio.channels.SocketChannel#read(ByteBuffer)
     */
    int read(ByteBuffer dst) throws IOException;

    /**
     * Will be removed in the future; only reason this method exists is for TLS.
     *
     * @see java.nio.channels.SocketChannel#write(ByteBuffer)
     */
    int write(ByteBuffer src) throws IOException;

    /**
     * Will be removed in the future; it a responsibility of the {@link EventLoopGroup} to poke around in e.g. the
     * {@link SocketChannel} to make it blocking or not.
     *
     * @see java.nio.channels.SocketChannel#configureBlocking(boolean)
     */
    SelectableChannel configureBlocking(boolean block) throws IOException;

    /**
     * @see java.nio.channels.SocketChannel#isOpen()
     */
    boolean isOpen();

    /**
     * Closes inbound.
     *
     * <p>Not thread safe. Should be called in appropriate io thread.</p>
     *
     * @throws IOException
     */
    void closeInbound() throws IOException;

    /**
     * Closes outbound.
     *
     * <p>Not thread safe. Should be called in appropriate io thread.</p>
     *
     * Will be removed in the future.
     *
     * @throws IOException
     */
    void closeOutbound() throws IOException;

    /**
     * @see java.nio.channels.SocketChannel#close()
     *
     * Will be removed in the future.
     */
    void close() throws IOException;

    void addCloseListener(CloseListener closeListener);

    /**
     * @return the last time in milliseconds a write to the network was made.
     */
    long lastWriteTimeMillis();

    /**
     * @return the last time in milliseconds a read from the network was done.
     */
    long lastReadTimeMillis();

    /**
     * Writes an OutboundFrame to this channel. The frame doesn't need to be written to socket directly, it is also
     * ok if the frame gets queued for writing at some point in time.
     *
     * Unlike the {@link #write(ByteBuffer)}, this method is thread-safe.
     *
     * No guarantee is made that the frame is going to be written or received even if the method returned true.
     *
     * @param frame the frame to write
     * @return false was not written because the channel got closed.
     */
    boolean write(OutboundFrame frame);
}
