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

package com.hazelcast.spi.discovery.multicast.impl;

import com.hazelcast.logging.ILogger;

import java.net.DatagramPacket;
import java.net.MulticastSocket;

public class MulticastDiscoveryReceiver {

    private static final int DATAGRAM_BUFFER_SIZE = 64 * 1024;

    private final MulticastSocket multicastSocket;
    private final DatagramPacket datagramPacketReceive = new DatagramPacket(new byte[DATAGRAM_BUFFER_SIZE], DATAGRAM_BUFFER_SIZE);
    private final ILogger logger;
    private final MulticastDiscoverySerializationHelper serializationHelper;

    public MulticastDiscoveryReceiver(MulticastSocket multicastSocket, ILogger logger,
            MulticastDiscoverySerializationHelper serializationHelper) {
        this.multicastSocket = multicastSocket;
        this.logger = logger;
        this.serializationHelper = serializationHelper;
    }

    public MulticastMemberInfo receive() {
        try {
            multicastSocket.receive(datagramPacketReceive);
            byte[] data = datagramPacketReceive.getData();
            return serializationHelper.deserialize(data);
        } catch (Exception e) {
            if (logger.isFinestEnabled()) {
                logger.finest("Couldn't get member info from multicast channel " + e.getMessage());
            }
        }
        return null;
    }
}
