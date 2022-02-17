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
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

import static java.lang.Thread.currentThread;

public class MulticastDiscoverySender implements Runnable {

    private static final int SLEEP_DURATION = 2000;
    private MulticastSocket multicastSocket;
    private MulticastMemberInfo multicastMemberInfo;
    private DatagramPacket datagramPacket;
    private final ILogger logger;
    private final String group;
    private final int port;
    private final MulticastDiscoverySerializationHelper serializationHelper;
    private volatile boolean stop;

    public MulticastDiscoverySender(DiscoveryNode discoveryNode, MulticastSocket multicastSocket, ILogger logger, String group,
            int port, MulticastDiscoverySerializationHelper serializationHelper) throws IOException {
        this.multicastSocket = multicastSocket;
        this.logger = logger;
        this.group = group;
        this.port = port;
        this.serializationHelper = serializationHelper;
        if (discoveryNode != null) {
            Address address = discoveryNode.getPublicAddress();
            multicastMemberInfo = new MulticastMemberInfo(address.getHost(), address.getPort());
        }
        initDatagramPacket();
    }

    private void initDatagramPacket() throws IOException {
        byte[] yourBytes = serializationHelper.serialize(multicastMemberInfo);
        datagramPacket = new DatagramPacket(yourBytes, yourBytes.length,
                InetAddress.getByName(group), port);
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                send();
            } catch (IOException e) {
                logger.finest(e.getMessage());
            }
            sleepUnlessStopped();
        }
    }

    private void sleepUnlessStopped() {
        if (stop) {
            return;
        }
        try {
            Thread.sleep(SLEEP_DURATION);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            logger.finest("Thread sleeping interrupted. This may due to graceful shutdown.");
        }
    }

    void send() throws IOException {
        multicastSocket.send(datagramPacket);
    }

    public void stop() {
        stop = true;
    }
}
