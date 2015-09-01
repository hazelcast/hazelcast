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

import com.hazelcast.nio.SocketWritable;

import java.nio.ByteBuffer;

/**
 * Responsible for writing {@link SocketWritable} to a {@link ByteBuffer}.
 *
 * Each {@link SocketWriter} will have its own {@link WriteHandler} instance. Therefor it doesn't need
 * to be thread-safe.
 *
 * For more information about the WriteHandler (and handlers in generally), have a look at the {@link ReadHandler}.
 *
 * @param <T>
 * @see IOThreadingModel
 */
public interface WriteHandler<T extends SocketWritable> {

    /**
     * A callback to indicate that the socketWritable should be written to the destination ByteBuffer.
     *
     * It could be that a SocketWritable is too big to fit into the ByteBuffer in 1 go; in that case this call will be made
     * for the same SocketWritable multiple times until write returns true. It is up to the SocketWritable to track where
     * it needs to continue.
     *
     * @param socketWritable the SocketWritable to write
     * @param dst            the destination ByteBuffer
     * @return true if the SocketWritable is completely written
     * @throws Exception if something fails while writing to ByteBuffer. When an exception is thrown, the TcpIpConnection is
     *                   closed. There is no point continuing with a potentially corrupted stream.
     */
    boolean onWrite(T socketWritable, ByteBuffer dst) throws Exception;
}
