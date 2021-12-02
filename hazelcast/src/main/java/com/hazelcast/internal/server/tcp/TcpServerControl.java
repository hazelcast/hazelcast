/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.cluster.impl.MemberHandshake;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.logging.ILogger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;

import static com.hazelcast.spi.properties.ClusterProperty.CHANNEL_COUNT;

/**
 * The ServerControl is responsible for sending/receiving TcpServerControl messages.
 */
public final class TcpServerControl {

    private final TcpServerConnectionManager connectionManager;
    private final ServerContext serverContext;
    private final ILogger logger;
    private final boolean unifiedEndpointManager;
    private final Set<ProtocolType> supportedProtocolTypes;
    private final int expectedPlaneCount;

    public TcpServerControl(TcpServerConnectionManager connectionManager,
                            ServerContext serverContext,
                            ILogger logger,
                            Set<ProtocolType> supportedProtocolTypes) {
        this.connectionManager = connectionManager;
        this.serverContext = serverContext;
        this.logger = logger;
        this.supportedProtocolTypes = supportedProtocolTypes;
        this.unifiedEndpointManager = connectionManager.getEndpointQualifier() == null;
        this.expectedPlaneCount = serverContext.properties().getInteger(CHANNEL_COUNT);
    }

    public void process(Packet packet) {
        MemberHandshake handshake = serverContext.getSerializationService().toObject(packet);
        TcpServerConnection connection = (TcpServerConnection) packet.getConn();
        if (!connection.setHandshake()) {
            if (logger.isFinestEnabled()) {
                logger.finest("Connection " + connection + " handshake is already completed, ignoring incoming " + handshake);
            }
            return;
        }

        if (handshake.getPlaneCount() != expectedPlaneCount) {
            connection.close("The connection handshake has incorrect number of planes. "
                    + "Expected " + expectedPlaneCount + " found " + handshake.getPlaneCount(), null);
            return;
        }

        // before we register the connection on the plane, we make sure the plane index is set on the connection
        // so that we can safely remove the connection from the plane.
        connection.setPlaneIndex(handshake.getPlaneIndex());
        process(connection, handshake);
    }

    private synchronized void process(TcpServerConnection connection, MemberHandshake handshake) {
        if (logger.isFinestEnabled()) {
            logger.finest("Handshake " + connection + ", complete message is " + handshake);
        }

        Map<ProtocolType, Collection<Address>> remoteAddressesPerProtocolType = handshake.getLocalAddresses();
        List<Address> allAliases = new ArrayList<>();
        for (Map.Entry<ProtocolType, Collection<Address>> remoteAddresses : remoteAddressesPerProtocolType.entrySet()) {
            if (supportedProtocolTypes.contains(remoteAddresses.getKey())) {
                allAliases.addAll(remoteAddresses.getValue());
            }
        }
        // member connections must be registered with their public address in connectionsMap
        // eg member 192.168.1.1:5701 initiates a connection to 192.168.1.2:5701; the connection
        // is created from an outbound port (eg 192.168.1.1:54003 --> 192.168.1.2:5701), but
        // in 192.168.1.2:5701's connectionsMap the connection must be registered with
        // key 192.168.1.1:5701.
        assert (connectionManager.getEndpointQualifier() != EndpointQualifier.MEMBER
                || connection.getConnectionType().equals(ConnectionType.MEMBER))
                : "When handling MEMBER connections, connection type"
                + " must be already set";
        boolean isMemberConnection = (connection.getConnectionType().equals(ConnectionType.MEMBER)
                && (connectionManager.getEndpointQualifier() == EndpointQualifier.MEMBER
                || unifiedEndpointManager));
        boolean mustRegisterRemoteSocketAddress = !handshake.isReply();

        Address remoteEndpoint = null;
        if (isMemberConnection) {
            // when a member connection is being bound on the connection initiator side
            // add the remote socket address as last alias. This way the intended public
            // address of the target member will be set correctly in TcpIpConnection.setEndpoint.
            if (mustRegisterRemoteSocketAddress) {
                allAliases.add(new Address(connection.getRemoteSocketAddress()));
            }
        } else {
            // when not a member connection, register the remote socket address
            remoteEndpoint = new Address(connection.getRemoteSocketAddress());
        }

        process0(connection, remoteEndpoint, allAliases, handshake);
    }

    /**
     * Performs the processing of the handshake (sets the endpoint on the Connection, registers the connection)
     * without any spoofing or other validation checks.
     * When executed on the connection initiator side, the connection is registered on the remote address
     * with which it was registered in {@link TcpServerConnectionManager#planes},
     * ignoring the {@code primaryAddress} argument.
     *
     * @param connection           the connection that send the handshake
     * @param remoteEndpointAddress       the address of the remote endpoint
     * @param remoteAddressAliases alias addresses as provided by the remote endpoint, under which the connection
     *                             will be registered. These are the public addresses configured on the remote.
     */
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    @SuppressFBWarnings("RV_RETURN_VALUE_OF_PUTIFABSENT_IGNORED")
    private synchronized void process0(TcpServerConnection connection,
                                       Address remoteEndpointAddress,
                                       Collection<Address> remoteAddressAliases,
                                       MemberHandshake handshake) {
        Address primaryAddress = remoteEndpointAddress;
        Address connectedAddress = new Address(connection.getRemoteSocketAddress());
        if (connectionManager.planes[handshake.getPlaneIndex()].hasConnectionInProgress(connectedAddress)) {
            // this is the connection initiator side --> register the connection under the address that was requested
            primaryAddress = connectedAddress;
        }
        UUID remoteUuid = handshake.getUuid();
        if (primaryAddress == null) {
            if (remoteAddressAliases == null) {
                throw new IllegalStateException("Remote endpoint and remote address aliases cannot be both null");
            } else {
                // let it fail if no primaryAddress and no aliases are defined
                primaryAddress = remoteAddressAliases.iterator().next();
            }
        }
        connection.setRemoteAddress(primaryAddress);
        serverContext.onSuccessfulConnection(primaryAddress);
        if (handshake.isReply()) {
            new SendMemberHandshakeTask(logger, serverContext, connection, primaryAddress, false,
                    handshake.getPlaneIndex(), handshake.getPlaneCount()).run();
        }

        if (checkAlreadyConnected(connection, remoteUuid, handshake.getPlaneIndex())) {
            return;
        }

        if (logger.isLoggable(Level.FINEST)) {
            logger.finest("Registering connection " + connection + " to address " + primaryAddress
                    + " planeIndex:" + handshake.getPlaneIndex());
        }
        boolean registered = connectionManager.register(primaryAddress, remoteUuid, connection, handshake.getPlaneIndex());
        if (registered) {
            LinkedAddresses addressesToRegister = LinkedAddresses.getAllLinkedAddresses(primaryAddress);
            addressesToRegister.addAddress(connectedAddress);
            if (remoteAddressAliases != null) {
                for (Address remoteAddressAlias : remoteAddressAliases) {
                    addressesToRegister.addAddress(remoteAddressAlias);
                }
            }
            connectionManager.addressRegistry.register(remoteUuid, addressesToRegister);
            if (logger.isFinestEnabled()) {
                logger.finest("Registered the addresses: " + addressesToRegister
                        + " for the member uuid=" + remoteUuid);
            }
        }
    }

    private boolean checkAlreadyConnected(TcpServerConnection connection, UUID remoteUuid, int planeIndex) {
        Connection existingConnection = connectionManager.planes[planeIndex].getConnection(remoteUuid);
        if (existingConnection != null && existingConnection.isAlive()) {
            if (existingConnection != connection) {
                if (logger.isFinestEnabled()) {
                    logger.finest(existingConnection + " is already bound to Member[uuid=" + remoteUuid
                            + "], new one is " + connection + " planeIndex:" + planeIndex);
                }
                // todo probably it's already in activeConnections (ConnectTask , AcceptorIOThread)
                connectionManager.connections.add(connection);
            }
            return true;
        }
        return false;
    }
}
