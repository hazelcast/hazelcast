/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

public interface SocketChannelWrapper {

    boolean isBlocking();

    int validOps();

    java.net.Socket socket();

    boolean isConnected();

    boolean isConnectionPending();

    boolean connect(java.net.SocketAddress socketAddress) throws java.io.IOException;

    boolean finishConnect() throws java.io.IOException;

    java.nio.channels.SelectionKey keyFor(java.nio.channels.Selector selector);

    java.nio.channels.SelectionKey register(java.nio.channels.Selector selector, int i, java.lang.Object o) throws ClosedChannelException;

    int read(java.nio.ByteBuffer byteBuffer) throws java.io.IOException;

    long read(java.nio.ByteBuffer[] byteBuffers, int i, int i1) throws java.io.IOException;

    long read(java.nio.ByteBuffer[] byteBuffers) throws java.io.IOException;

    int write(java.nio.ByteBuffer byteBuffer) throws java.io.IOException;

    long write(java.nio.ByteBuffer[] byteBuffers, int i, int i1) throws java.io.IOException;

    long write(java.nio.ByteBuffer[] byteBuffers) throws java.io.IOException;

    java.nio.channels.SelectableChannel configureBlocking(boolean b) throws java.io.IOException;

    boolean isOpen();

    void close() throws IOException;
}
