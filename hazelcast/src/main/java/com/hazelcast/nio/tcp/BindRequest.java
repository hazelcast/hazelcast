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

package com.hazelcast.nio.tcp;

import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.cluster.impl.BindMessage;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.cluster.impl.ExtendedBindMessage;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Packet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class BindRequest {

    private final ILogger logger;

    private final IOService ioService;

    private final TcpIpConnection connection;
    private final Address remoteEndPoint;
    private final boolean reply;

    BindRequest(ILogger logger, IOService ioService, TcpIpConnection connection, Address remoteEndPoint, boolean reply) {
        this.logger = logger;
        this.ioService = ioService;
        this.connection = connection;
        this.remoteEndPoint = remoteEndPoint;
        this.reply = reply;
    }

    public void send() {
        connection.setEndPoint(remoteEndPoint);
        ioService.onSuccessfulConnection(remoteEndPoint);
        //make sure bind packet is the first packet sent to the end point.
        if (logger.isFinestEnabled()) {
            logger.finest("Sending bind packet to " + remoteEndPoint);
        }
        // since 3.12, send the new bind message followed by the pre-3.12 BindMessage
        ExtendedBindMessage bind = new ExtendedBindMessage((byte) 1, getConfiguredLocalAddresses(), remoteEndPoint, reply);
        byte[] bytes = ioService.getSerializationService().toBytes(bind);
        // using one of the undefined packet types we can avoid old members
        // logging a serialization exception because they cannot deserialize the
        // ExtendedBindMessage
        // instead, a SEVERE entry about undefined packet type will be logged
        Packet packet = new Packet(bytes).setPacketType(Packet.Type.EXTENDED_BIND);
        connection.write(packet);

        BindMessage oldbind = new BindMessage(ioService.getThisAddress(), remoteEndPoint, reply);
        bytes = ioService.getSerializationService().toBytes(oldbind);
        packet = new Packet(bytes).setPacketType(Packet.Type.BIND);
        connection.write(packet);
        //now you can send anything...
    }

    Map<ProtocolType, Collection<Address>> getConfiguredLocalAddresses() {
        Map<ProtocolType, Collection<Address>> addressMap = new HashMap<ProtocolType, Collection<Address>>();
        Map<EndpointQualifier, Address> addressesPerEndpointQualifier = ioService.getThisAddresses();
        for (Map.Entry<EndpointQualifier, Address> addressEntry : addressesPerEndpointQualifier.entrySet()) {
            Collection<Address> addresses = addressMap.get(addressEntry.getKey().getType());
            if (addresses == null) {
                addresses = new ArrayList<Address>();
                addressMap.put(addressEntry.getKey().getType(), addresses);
            }
            addresses.add(addressEntry.getValue());
        }
        return addressMap;
    }
}
