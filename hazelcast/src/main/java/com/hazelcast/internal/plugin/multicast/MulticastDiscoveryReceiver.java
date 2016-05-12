/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.plugin.multicast;

import com.hazelcast.nio.IOUtil;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.MulticastSocket;

public class MulticastDiscoveryReceiver {

    MulticastSocket multicastSocket;
    private static final int DATAGRAM_BUFFER_SIZE = 64 * 1024;
    DatagramPacket datagramPacketReceive = new DatagramPacket(new byte[DATAGRAM_BUFFER_SIZE], DATAGRAM_BUFFER_SIZE);


    public MulticastDiscoveryReceiver(MulticastSocket multicastSocket) {
        this.multicastSocket = multicastSocket;
    }

    public MulticastMemberInfo receive() {
        try {
            Object o;
            multicastSocket.receive(datagramPacketReceive);
            byte[] data = datagramPacketReceive.getData();
            MulticastMemberInfo multicastMemberInfo;
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            ObjectInputStream in;
            in = new ObjectInputStream(bis);
            o = in.readObject();
            multicastMemberInfo = (MulticastMemberInfo) o;
            IOUtil.closeResource(bis);
            IOUtil.closeResource(in);
            return multicastMemberInfo;
        } catch (Exception e) {
            // ignore exception
        }
        return null;
    }
}
