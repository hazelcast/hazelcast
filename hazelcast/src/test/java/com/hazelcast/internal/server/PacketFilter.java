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

package com.hazelcast.internal.server;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Packet;

/**
 * A filter used by network system to allow, reject, drop or delay {@code Packet}s
 */
public interface PacketFilter {

    enum Action {
        /**
         * Allows the packet to pass through intact
         */
        ALLOW,

        /**
         * Rejects the packet. Sender will be aware of rejection.
         */
        REJECT,

        /**
         * Silently drops the packet as if sent to the destination.
         */
        DROP,

        /**
         * Delays sending packet to the destination.
         */
        DELAY
    }

    /**
     * Filters a packet inspecting its content and/or endpoint and decides
     * whether this packet should be filtered.
     *
     * @param packet   packet
     * @param endpoint target endpoint which packet is sent to
     * @return returns An {@link Action} to denote whether packet should pass through intact,
     * or should be rejected/dropped/delayed
     */
    Action filter(Packet packet, Address endpoint);

}
