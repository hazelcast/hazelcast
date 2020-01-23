/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.Data;

/**
 * Represents a payload to can be written to a {@link Channel}
 *
 * There are different types of OutboundFrame:
 * <ol>
 * <li>Packet: for member to member and old-client to member communication</li>
 * <li>TextMessage: for memcached and rest communication</li>
 * <li>ClientMessage: for the new client to member communication</li>
 * </ol>
 *
 * Till so far, all communication over a single connection, will be of a single
 * Frame-class. E.g. member to member only uses Packets.
 *
 * There is no need for an InboundFrame interface.
 *
 * @see Data
 * @see Channel#write(OutboundFrame)
 */
public interface OutboundFrame {

    /**
     * Checks if this Frame is urgent.
     *
     * Frames that are urgent, have priority above regular frames. This is useful
     * to implement System Operations so that they can be send faster than regular
     * operations; especially when the system is under load you want these operations
     * have precedence.
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
