/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nio.tcp;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.cluster.impl.BindMessage;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.nio.IOService;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.logging.ILogger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

final class BindHandler {

    private final TcpIpEndpointManager tcpIpEndpointManager;
    private final IOService ioService;
    private final ILogger logger;
    private final boolean spoofingChecks;
    private final boolean unifiedEndpointManager;

    private final Set<ProtocolType> supportedProtocolTypes;

    BindHandler(TcpIpEndpointManager tcpIpEndpointManager, IOService ioService, ILogger logger,
                boolean spoofingChecks, Set<ProtocolType> supportedProtocolTypes) {
        this.tcpIpEndpointManager = tcpIpEndpointManager;
        this.ioService = ioService;
        this.logger = logger;
        this.spoofingChecks = spoofingChecks;
        this.supportedProtocolTypes = supportedProtocolTypes;
        this.unifiedEndpointManager = tcpIpEndpointManager.getEndpointQualifier() == null;
    }

    public void process(Packet packet) {
        Object bind = ioService.getSerializationService().toObject(packet);
        TcpIpConnection connection = (TcpIpConnection) packet.getConn();
        if (connection.setBinding()) {
            BindMessage bindMessage = (BindMessage) bind;
            bind(connection, bindMessage);
        } else {
            if (logger.isFinestEnabled()) {
                logger.finest("Connection " + connection + " is already bound, ignoring incoming " + bind);
            }
        }
    }

    private synchronized boolean bind(TcpIpConnection connection, BindMessage bindMessage) {
        if (logger.isFinestEnabled()) {
            logger.finest("Binding " + connection + ", complete message is " + bindMessage);
        }

        Map<ProtocolType, Collection<Address>> remoteAddressesPerProtocolType = bindMessage.getLocalAddresses();
        List<Address> allAliases = new ArrayList<Address>();
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
        assert (tcpIpEndpointManager.getEndpointQualifier() != EndpointQualifier.MEMBER
                || connection.getType() == ConnectionType.MEMBER) : "When handling MEMBER connections, connection type"
                + " must be already set";
        boolean isMemberConnection = (connection.getType() == ConnectionType.MEMBER
                && (tcpIpEndpointManager.getEndpointQualifier() == EndpointQualifier.MEMBER
                    || unifiedEndpointManager));
        boolean mustRegisterRemoteSocketAddress = !bindMessage.isReply();

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

        return bind0(connection,
                remoteEndpoint,
                allAliases,
                bindMessage.isReply());
    }

    /**
     * Performs the actual binding (sets the endpoint on the Connection, registers the connection)
     * without any spoofing or other validation checks.
     * When executed on the connection initiator side, the connection is registered on the remote address
     * with which it was registered in {@link TcpIpEndpointManager#connectionsInProgress},
     * ignoring the {@code remoteEndpoint} argument.
     *
     * @param connection            the connection to bind
     * @param remoteEndpoint        the address of the remote endpoint
     * @param remoteAddressAliases  alias addresses as provided by the remote endpoint, under which the connection
     *                              will be registered. These are the public addresses configured on the remote.
     */
    @SuppressWarnings({"checkstyle:npathcomplexity"})
    @SuppressFBWarnings("RV_RETURN_VALUE_OF_PUTIFABSENT_IGNORED")
    private synchronized boolean bind0(TcpIpConnection connection, Address remoteEndpoint,
                                       Collection<Address> remoteAddressAliases, boolean reply) {
        final Address remoteAddress = new Address(connection.getRemoteSocketAddress());
        if (tcpIpEndpointManager.connectionsInProgress.contains(remoteAddress)) {
            // this is the connection initiator side --> register the connection under the address that was requested
            remoteEndpoint = remoteAddress;
        }
        if (remoteEndpoint == null) {
            if (remoteAddressAliases == null) {
                throw new IllegalStateException("Remote endpoint and remote address aliases cannot be both null");
            } else {
                // let it fail if no remoteEndpoint and no aliases are defined
                remoteEndpoint = remoteAddressAliases.iterator().next();
            }
        }
        connection.setEndPoint(remoteEndpoint);
        ioService.onSuccessfulConnection(remoteEndpoint);
        if (reply) {
            BindRequest bindRequest = new BindRequest(logger, ioService, connection, remoteEndpoint, false);
            bindRequest.send();
        }

        if (checkAlreadyConnected(connection, remoteEndpoint)) {
            return false;
        }

        if (logger.isLoggable(Level.FINEST)) {
            logger.finest("Registering connection " + connection + " to address " + remoteEndpoint);
        }
        boolean returnValue = tcpIpEndpointManager.registerConnection(remoteEndpoint, connection);

        if (remoteAddressAliases != null && returnValue) {
            for (Address remoteAddressAlias : remoteAddressAliases) {
                if (logger.isLoggable(Level.FINEST)) {
                    logger.finest("Registering connection " + connection + " to address alias " + remoteAddressAlias);
                }
                tcpIpEndpointManager.connectionsMap.putIfAbsent(remoteAddressAlias, connection);
            }
        }

        return returnValue;
    }

    private boolean checkAlreadyConnected(TcpIpConnection connection, Address remoteEndPoint) {
        final Connection existingConnection = tcpIpEndpointManager.getConnection(remoteEndPoint);
        if (existingConnection != null && existingConnection.isAlive()) {
            if (existingConnection != connection) {
                if (logger.isFinestEnabled()) {
                    logger.finest(existingConnection + " is already bound to " + remoteEndPoint + ", new one is " + connection);
                }
                // todo probably it's already in activeConnections (ConnectTask , AcceptorIOThread)
                tcpIpEndpointManager.activeConnections.add(connection);
            }
            return true;
        }
        return false;
    }

}
