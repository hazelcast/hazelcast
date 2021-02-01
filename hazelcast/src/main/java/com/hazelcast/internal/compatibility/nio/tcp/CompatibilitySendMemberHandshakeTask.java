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

package com.hazelcast.internal.compatibility.nio.tcp;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.compatibility.cluster.impl.CompatibilityExtendedBindMessage;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.Packet.Type;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import com.hazelcast.logging.ILogger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.nio.Packet.FLAG_4_0;

/**
 * Compatibility bind request (sent from a 3.x member) that is, for all
 * intents and purposes, equal to the
 * {@link com.hazelcast.internal.server.tcp.SendMemberHandshakeTask}.
 * The difference is in the serialization format and some fields.
 */
public class CompatibilitySendMemberHandshakeTask implements Runnable {

    private final ILogger logger;
    private final ServerContext serverContext;
    private final TcpServerConnection connection;
    private final Address remoteEndPoint;
    private final boolean reply;

    public CompatibilitySendMemberHandshakeTask(ILogger logger,
                                                ServerContext serverContext,
                                                TcpServerConnection connection,
                                                Address remoteEndPoint,
                                                boolean reply) {
        this.logger = logger;
        this.serverContext = serverContext;
        this.connection = connection;
        this.remoteEndPoint = remoteEndPoint;
        this.reply = reply;
    }

    @Override
    public void run() {
        connection.setRemoteAddress(remoteEndPoint);
        serverContext.onSuccessfulConnection(remoteEndPoint);
        //make sure bind packet is the first packet sent to the end point.
        if (logger.isFinestEnabled()) {
            logger.finest("Sending bind packet to " + remoteEndPoint);
        }
        // since we only support connecting to 3.12, we will only send
        // the 3.12 ExtendedBindMessage ("new bind message") and we skip
        // sending the "old" BindMessage.
        CompatibilityExtendedBindMessage bind =
                new CompatibilityExtendedBindMessage((byte) 1, getConfiguredLocalAddresses(), remoteEndPoint, reply);
        byte[] bytes = serverContext.getSerializationService().toBytes(bind);
        Packet packet = new Packet(bytes).setPacketType(Type.COMPATIBILITY_EXTENDED_BIND);
        // unset 4_0 flag
        packet.resetFlagsTo(packet.getFlags() & ~FLAG_4_0);
        connection.write(packet);

        //now you can send anything...
    }

    Map<ProtocolType, Collection<Address>> getConfiguredLocalAddresses() {
        Map<ProtocolType, Collection<Address>> addressMap = new HashMap<>();
        Map<EndpointQualifier, Address> addressesPerEndpointQualifier = serverContext.getThisAddresses();
        for (Map.Entry<EndpointQualifier, Address> addressEntry : addressesPerEndpointQualifier.entrySet()) {
            Collection<Address> addresses = addressMap.get(addressEntry.getKey().getType());
            if (addresses == null) {
                addresses = new ArrayList<>();
                addressMap.put(addressEntry.getKey().getType(), addresses);
            }
            addresses.add(addressEntry.getValue());
        }
        return addressMap;
    }
}
