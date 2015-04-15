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

package com.hazelcast.spi.impl.transceiver;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;

/**
 * The PacketTransceiver is responsible for receiving and transmitting {@link com.hazelcast.nio.Packet} instances.
 *
 * When it receives a Packet (from the IO system) it dispatches to the appropriate service:
 * <ol>
 *     <li>OperationService</li>
 *     <li>EventService</li>
 *     <li>WanReplicationService</li>
 * </ol>
 *
 * Sending is a bit simpler since the connection is looked up if needed and then send to the other side.
 *
 * The name transceiver comes from the radio and networking terminology and is a combination of transmitting and
 * receiving.
 */
public interface PacketTransceiver {

    /**
     * Transmits a packet to a certain connection.
     *
     * @param packet The Packet to transmit.
     * @param connection The connection to where the Packet should be transmitted.
     * @return true if the transmit was a success, false if a failure. There is no guarantee that the packet is actually going
     * to be received since the Packet perhaps is stuck in some buffer. It just means that it is buffered somewhere.
     */
    boolean transmit(Packet packet, Connection connection);

    /**
     * Transmits a packet to a certain address.
     *
     * @param packet The Packet to transmit.
     * @param target The address of the target machine where the Packet should be transmitted.
     * @return true if the transmit was a success, false if a failure.
     * @see #transmit(com.hazelcast.nio.Packet, com.hazelcast.nio.Connection)
     */
    boolean transmit(Packet packet, Address target);

    /**
     * Lets the PacketTransceiver receive a packet.
     *
     * This method is likely going to be called from the IO system when it takes a Packet of the wire and offers it to be
     * processed by the system. This is where the dispatching (demultiplexing) to the appropriate services takes place.
     *
     * @param packet the Packet to receive.
     */
    void receive(Packet packet);
}
