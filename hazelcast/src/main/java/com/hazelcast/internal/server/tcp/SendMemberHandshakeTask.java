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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.cluster.impl.MemberHandshake;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.logging.ILogger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.cluster.impl.MemberHandshake.OPTION_PLANE_COUNT;
import static com.hazelcast.internal.cluster.impl.MemberHandshake.OPTION_PLANE_INDEX;
import static com.hazelcast.internal.cluster.impl.MemberHandshake.SCHEMA_VERSION_2;

public class SendMemberHandshakeTask implements Runnable {

    private final ILogger logger;
    private final ServerContext serverContext;
    private final TcpServerConnection connection;
    private final Address remoteAddress;
    private final boolean reply;
    private final int planeIndex;
    private final int planeCount;

    public SendMemberHandshakeTask(ILogger logger,
                                   ServerContext serverContext,
                                   TcpServerConnection connection,
                                   Address remoteAddress,
                                   boolean reply,
                                   int planeIndex,
                                   int planeCount) {
        this.logger = logger;
        this.serverContext = serverContext;
        this.connection = connection;
        this.remoteAddress = remoteAddress;
        this.reply = reply;
        this.planeIndex = planeIndex;
        this.planeCount = planeCount;
    }

    @Override
    public void run() {
        connection.setRemoteAddress(remoteAddress);
        serverContext.onSuccessfulConnection(remoteAddress);
        //make sure memberHandshake packet is the first packet sent to the end point.
        if (logger.isFinestEnabled()) {
            logger.finest("Sending memberHandshake packet to " + remoteAddress);
        }
        MemberHandshake memberHandshake = new MemberHandshake(
                SCHEMA_VERSION_2,
                getConfiguredLocalAddresses(),
                remoteAddress,
                reply,
                serverContext.getThisUuid())
                .addOption(OPTION_PLANE_COUNT, planeCount)
                .addOption(OPTION_PLANE_INDEX, planeIndex);
        byte[] bytes = serverContext.getSerializationService().toBytes(memberHandshake);
        Packet packet = new Packet(bytes).setPacketType(Packet.Type.SERVER_CONTROL);
        connection.write(packet);
        //now you can send anything...
    }

    Map<ProtocolType, Collection<Address>> getConfiguredLocalAddresses() {
        Map<ProtocolType, Collection<Address>> addressMap = new HashMap<ProtocolType, Collection<Address>>();
        Map<EndpointQualifier, Address> addressesPerEndpointQualifier = serverContext.getThisAddresses();
        for (Map.Entry<EndpointQualifier, Address> addressEntry : addressesPerEndpointQualifier.entrySet()) {
            Collection<Address> addresses = addressMap.computeIfAbsent(addressEntry.getKey().getType(), k -> new ArrayList<>());
            addresses.add(addressEntry.getValue());
        }
        return addressMap;
    }
}
