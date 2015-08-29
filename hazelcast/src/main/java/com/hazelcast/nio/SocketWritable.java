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
 * Represents something that can be written to a {@link com.hazelcast.nio.Connection}.
 *
 * @see SocketReadable
 * @see com.hazelcast.nio.serialization.Data
 * @see Connection#write(SocketWritable)
 */
public interface SocketWritable {

    /**
     * Asks the SocketWritable to write its content to the destination ByteBuffer.
     *
     * As long as the writeTo returns false, this SocketWritable is not yet finished. E.g. it could be the SocketWritable
     * contains 1 MB of data, but if 100KB ByteBuffer is passed, 10 calls writeTo calls are needed, where the first 9 return
     * false and the 10th returns true.
     *
     * It is up to the SocketWritable to keep track of where it is in the writing process. For this reason a SocketWritable
     * can't be shared by multiple threads.
     *
     * @param dst the ByteBuffer to write to.
     * @return true if the object is fully written.
     */
    boolean writeTo(ByteBuffer dst);

    /**
     * Checks if this SocketWritable is urgent.
     *
     * SocketWritable that are urgent, have priority above regular SocketWritable. This is useful to implement
     * System Operations so that they can be send faster than regular operations; especially when the system is
     * under load you want these operations have precedence.
     *
     * @return true if urgent, false otherwise.
     */
    boolean isUrgent();
}
