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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.cluster.impl.MemberHandshake;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.ClusterProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

/**
 * A {@link ChannelInitializer} that runs on a member and used for unencrypted
 * channels. It will deal with the exchange of protocols and based on that it
 * will set up the appropriate handlers in the pipeline.
 */
public abstract class AbstractChannelInitializer
        implements ChannelInitializer {

    protected final ServerContext serverContext;
    private final EndpointConfig config;

    protected AbstractChannelInitializer(ServerContext serverContext, EndpointConfig config) {
        this.config = config;
        this.serverContext = serverContext;
    }

    public static final class MemberHandshakeHandler {

        private final TcpServerConnectionManager connectionManager;
        private final ServerContext serverContext;
        private final ILogger logger;
        private final boolean spoofingChecks;
        private final boolean unifiedEndpointManager;

        private final Set<ProtocolType> supportedProtocolTypes;

        public MemberHandshakeHandler(TcpServerConnectionManager connectionManager, ServerContext serverContext, ILogger logger,
                                      Set<ProtocolType> supportedProtocolTypes) {
            this.connectionManager = connectionManager;
            this.serverContext = serverContext;
            this.logger = logger;
            this.spoofingChecks = serverContext.properties().getBoolean(ClusterProperty.BIND_SPOOFING_CHECKS);
            this.supportedProtocolTypes = supportedProtocolTypes;
            this.unifiedEndpointManager = connectionManager.getEndpointQualifier() == null;
        }

        public void process(Packet packet) {
            Object o = serverContext.getSerializationService().toObject(packet);
            TcpServerConnection connection = (TcpServerConnection) packet.getConn();
            if (connection.setHandshake()) {
                MemberHandshake handshake = (MemberHandshake) o;
                process(connection, handshake);
            } else {
                if (logger.isFinestEnabled()) {
                    logger.finest("Connection " + connection + " is already bound, ignoring incoming " + o);
                }
            }
        }

        private synchronized boolean process(TcpServerConnection connection, MemberHandshake handshake) {
            if (logger.isFinestEnabled()) {
                logger.finest("Handshake " + connection + ", complete message is " + handshake);
            }

            Map<ProtocolType, Collection<Address>> remoteAddressesPerProtocolType = handshake.getLocalAddresses();
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

            return process0(connection, remoteEndpoint, allAliases, handshake.isReply());
        }

        /**
         * Performs the processing of the handshake (sets the endpoint on the Connection, registers the connection)
         * without any spoofing or other validation checks.
         * When executed on the connection initiator side, the connection is registered on the remote address
         * with which it was registered in {@link TcpServerConnectionManager#connectionsInProgress},
         * ignoring the {@code remoteEndpoint} argument.
         *
         * @param connection           the connection that send the handshake
         * @param remoteEndpoint       the address of the remote endpoint
         * @param remoteAddressAliases alias addresses as provided by the remote endpoint, under which the connection
         *                             will be registered. These are the public addresses configured on the remote.
         */
        @SuppressWarnings({"checkstyle:npathcomplexity"})
        @SuppressFBWarnings("RV_RETURN_VALUE_OF_PUTIFABSENT_IGNORED")
        private synchronized boolean process0(TcpServerConnection connection, Address remoteEndpoint,
                                              Collection<Address> remoteAddressAliases, boolean reply) {
            final Address remoteAddress = new Address(connection.getRemoteSocketAddress());
            if (connectionManager.connectionsInProgress.contains(remoteAddress)) {
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
            connection.setRemoteAddress(remoteEndpoint);
            serverContext.onSuccessfulConnection(remoteEndpoint);
            if (reply) {
                new SendMemberHandshakeTask(logger, serverContext, connection, remoteEndpoint, false).run();
            }

            if (checkAlreadyConnected(connection, remoteEndpoint)) {
                return false;
            }

            if (logger.isLoggable(Level.FINEST)) {
                logger.finest("Registering connection " + connection + " to address " + remoteEndpoint);
            }
            boolean returnValue = connectionManager.register(remoteEndpoint, connection);

            if (remoteAddressAliases != null && returnValue) {
                for (Address remoteAddressAlias : remoteAddressAliases) {
                    if (logger.isLoggable(Level.FINEST)) {
                        logger.finest("Registering connection " + connection + " to address alias " + remoteAddressAlias);
                    }
                    connectionManager.mappedConnections.putIfAbsent(remoteAddressAlias, connection);
                }
            }

            return returnValue;
        }

        private boolean checkAlreadyConnected(TcpServerConnection connection, Address remoteEndPoint) {
            final Connection existingConnection = connectionManager.get(remoteEndPoint);
            if (existingConnection != null && existingConnection.isAlive()) {
                if (existingConnection != connection) {
                    if (logger.isFinestEnabled()) {
                        logger.finest(existingConnection + " is already bound to " + remoteEndPoint
                                + ", new one is " + connection);
                    }
                    // todo probably it's already in activeConnections (ConnectTask , AcceptorIOThread)
                    connectionManager.connections.add(connection);
                }
                return true;
            }
            return false;
        }

    }
}
