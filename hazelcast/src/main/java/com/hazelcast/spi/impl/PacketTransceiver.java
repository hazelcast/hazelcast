package com.hazelcast.spi.impl;

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
     * @param packet the Packet to transmit
     * @param connection the Connection the Packet should be transmitted to.
     * @return true if the transmit was a success, false if a failure. There is no guarantee that the packet is actually going
     * to be received since the Packet perhaps is stuck in some buffer. It just means that it is buffered somewhere.
     */
    boolean transmit(Packet packet, Connection connection);

    /**
     * Transmits a packet to a certain address
     *
     * @param packet the Packet to transmit
     * @param target the Address of the target machine the Packet should be transmitted to.
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
