/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import java.nio.channels.SocketChannel;

/**
 * Wraps a {@link java.nio.channels.SocketChannel}.
 *
 * The reason this class exists is because for enterprise encryption. Ideally the SocketChannel should have been decorated
 * with this encryption functionality, but unfortunately that isn't possible with this class.
 *
 * That is why a new 'wrapper' interface is introduced which acts like a SocketChannel and the implementations wrap a
 * SocketChannel.
 */
public interface SocketChannelWrapper extends Closeable {

    /**
     * @see java.nio.channels.SocketChannel#isOpen()
     */
    boolean isBlocking();

    /**
     * @see java.nio.channels.SocketChannel#socket()
     */
    Socket socket();

    /**
     * @see SocketChannel#isConnected() ()
     */
    boolean isConnected();

    /**
     * @see java.nio.channels.SocketChannel#connect(java.net.SocketAddress)
     */
    boolean connect(java.net.SocketAddress socketAddress) throws IOException;

    /**
     * @see java.nio.channels.SocketChannel#keyFor(Selector)
     */
    SelectionKey keyFor(Selector selector);

    /**
     * @see java.nio.channels.SocketChannel#register(Selector, int, Object)
     */
    SelectionKey register(Selector selector, int ops, Object attachment) throws ClosedChannelException;

    /**
     * @see java.nio.channels.SocketChannel#read(ByteBuffer)
     */
    int read(ByteBuffer dst) throws IOException;

    /**
     * @see java.nio.channels.SocketChannel#write(ByteBuffer)
     */
    int write(ByteBuffer src) throws IOException;

    /**
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
     * <p>Not thread safe. Should be called in channel reader thread.</p>
     *
     * @throws IOException
     */
    void closeInbound() throws IOException;

    /**
     * Closes outbound.
     *
     * <p>Not thread safe. Should be called in channel writer thread.</p>
     *
     * @throws IOException
     */
    void closeOutbound() throws IOException;

    /**
     * @see java.nio.channels.SocketChannel#close()
     */
    void close() throws IOException;
}
