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

import com.hazelcast.spi.discovery.DiscoveryNode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class MulticastDiscoverySender implements Runnable {

    DiscoveryNode discoveryNode;
    MulticastSocket multicastSocket;
    MulticastMemberInfo multicastMemberInfo;
    DatagramPacket datagramPacket;
    private boolean stop = false;

    public MulticastDiscoverySender(DiscoveryNode discoveryNode, MulticastSocket multicastSocket) throws IOException {
        this.discoveryNode = discoveryNode;
        this.multicastSocket = multicastSocket;
        if (discoveryNode != null) {
            multicastMemberInfo = new MulticastMemberInfo(discoveryNode.getPublicAddress().getHost(), discoveryNode.getPublicAddress().getPort());
        }
        initDatagramPacket();
    }

    private void initDatagramPacket() throws IOException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out;
        out = new ObjectOutputStream(bos);
        out.writeObject(multicastMemberInfo);
        byte[] yourBytes = bos.toByteArray();
        datagramPacket = new DatagramPacket(yourBytes, yourBytes.length, InetAddress
                .getByName("224.2.2.3"), 54327);
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                send();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    void send() throws IOException {
        multicastSocket.send(datagramPacket);
    }

    void stop() {
        stop = true;
    }
}
