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

package com.hazelcast.nio;

import java.nio.ByteBuffer;

/**
 * Represents a data-structure that can reconstruct itself based on the content of a byte-buffer.
 *
 * @see SocketWritable
 * @see Packet
 */
public interface SocketReadable {

    /**
     * Reads from the src.
     *
     * As long as the readFrom returns false, this SocketReadable is not yet finished. E.g. it could be the SocketReadable
     * requires 1 MB of data, but if 100KB ByteBuffer is passed, 10 calls readFrom calls are needed, where the first 9 return
     * false and the 10th returns true.
     *
     * It is up to the SocketReadable to keep track of where it is in the reading process.
     *
     * @param src the ByteBuffer to read from.
     * @return true if the object has been fully read and no more readFrom calls are needed.
     */
    boolean readFrom(ByteBuffer src);
}
