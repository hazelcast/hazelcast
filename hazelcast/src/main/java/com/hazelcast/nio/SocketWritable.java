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

package com.hazelcast.nio;

import java.nio.ByteBuffer;

/**
 * Represents something that can be written to a {@link com.hazelcast.nio.Connection}.
 *
 * todo:
 * Perhaps this class should be renamed to ConnectionWritable since it is written to a
 * {@link com.hazelcast.nio.Connection#write(SocketWritable)}. This aligns the names.
 */
public interface SocketWritable {

    /**
     * Asks the SocketWritable to write its content to the destination ByteBuffer.
     *
     * @param destination the ByteBuffer to write to.
     * @return todo: unclear what return value means.
     */
    boolean writeTo(ByteBuffer destination);

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
