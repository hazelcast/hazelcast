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

package com.hazelcast.spi.discovery.multicast.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.nio.IOUtil;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.MulticastSocket;

public class MulticastDiscoveryReceiver {

    private static final int DATAGRAM_BUFFER_SIZE = 64 * 1024;

    private final MulticastSocket multicastSocket;
    private final DatagramPacket datagramPacketReceive = new DatagramPacket(new byte[DATAGRAM_BUFFER_SIZE], DATAGRAM_BUFFER_SIZE);
    private final ILogger logger;

    public MulticastDiscoveryReceiver(MulticastSocket multicastSocket, ILogger logger) {
        this.multicastSocket = multicastSocket;
        this.logger = logger;
    }

    public MulticastMemberInfo receive() {
        ObjectInputStream in = null;
        ByteArrayInputStream bis = null;
        try {
            Object o;
            multicastSocket.receive(datagramPacketReceive);
            byte[] data = datagramPacketReceive.getData();
            MulticastMemberInfo multicastMemberInfo;
            bis = new ByteArrayInputStream(data);
            in = new ObjectInputStream(bis);
            o = in.readObject();
            multicastMemberInfo = (MulticastMemberInfo) o;
            return multicastMemberInfo;
        } catch (Exception e) {
            if (logger.isFinestEnabled()) {
                logger.finest("Couldn't get member info from multicast channel " + e.getMessage());
            }
        } finally {
            IOUtil.closeResource(bis);
            IOUtil.closeResource(in);
        }
        return null;
    }
}
