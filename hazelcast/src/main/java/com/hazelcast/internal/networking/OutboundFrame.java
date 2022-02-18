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

package com.hazelcast.internal.networking;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.internal.ascii.TextCommand;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.Data;

/**
 * Represents a payload that can be written to a {@link Channel}.
 * <p>
 * There are different types of OutboundFrame:
 * <ol>
 * <li> {@link Packet}: for member to member communication</li>
 * <li> {@link TextCommand}: for memcached and REST communication</li>
 * <li> {@link ClientMessage}: for the new client to member communication</li>
 * </ol>
 * <p>
 * Currently, all communication over a single connection is of a single
 * subclass. E.g. member to member only uses {@link Packet}.
 * <p>
 * There is no need for an InboundFrame interface.
 *
 * @see Data
 * @see Channel#write(OutboundFrame)
 */
public interface OutboundFrame {

    /**
     * Checks if this OutboundFrame is urgent.
     * <p>
     * Frames that are urgent, have priority above regular frames. This is useful
     * to implement system operations so that they can be sent faster than regular
     * operations; especially when the system is under load you want these operations
     * to have precedence.
     *
     * @return true if urgent, false otherwise.
     */
    boolean isUrgent();

    /**
     * Returns the frame length. This includes header and payload size.
     *
     * @return The frame length.
     */
    int getFrameLength();
}
