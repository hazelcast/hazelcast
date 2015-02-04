package com.hazelcast.spi.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.annotation.PrivateApi;

/**
 * The PacketTransceiver is responsible for receiving and transmitting {@link com.hazelcast.nio.Packet} instances.
 *
 * The name transceiver comes
 *
 * When it receives a Packet (from the IO system) it dispatches to the appropriate service:
 * <ol>
 *     <li>OperationService</li>
 *     <li>EventService</li>
 *     <li>WanReplicationService</li>
 * </ol>
 *
 * Sending is a bit simpler since the connection is looked up if needed and then send to the other side.
 */
public interface PacketTransceiver {

    boolean transmit(Packet packet, Connection connection);

    boolean transmit(Packet packet, Address target);

    void receive(Packet packet);
}
