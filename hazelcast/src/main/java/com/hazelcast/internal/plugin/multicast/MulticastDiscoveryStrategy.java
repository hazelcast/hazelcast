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

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.spi.partitiongroup.PartitionGroupStrategy;

import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class MulticastDiscoveryStrategy implements DiscoveryStrategy {

    private DiscoveryNode discoveryNode;
    private MulticastSocket multicastSocket;
    Thread t;
    private Map<String, Comparable> properties;
    private static final int DATA_OUTPUT_BUFFER_SIZE = 1024;
    boolean isClient;

    private MulticastDiscoveryReceiver multicastDiscoveryReceiver;
    private MulticastDiscoverySender multicastDiscoverySender;

    public MulticastDiscoveryStrategy(DiscoveryNode discoveryNode, Map<String, Comparable> properties) {
        this.discoveryNode = discoveryNode;
        this.properties = properties;

    }

    private void initializeMulticastSocket() {
        try {
            int port = getOrDefault(MulticastProperties.PORT, 54327);
            String group = getOrDefault(MulticastProperties.GROUP, "224.2.2.3");
            multicastSocket = new MulticastSocket(port);
            multicastSocket.setReuseAddress(true);
            multicastSocket.setTimeToLive(255);
            multicastSocket.setReceiveBufferSize(64 * DATA_OUTPUT_BUFFER_SIZE);
            multicastSocket.setSendBufferSize(64 * DATA_OUTPUT_BUFFER_SIZE);
            multicastSocket.setSoTimeout(3000);
            multicastSocket.joinGroup(InetAddress.getByName(group));
            multicastDiscoverySender = new MulticastDiscoverySender(discoveryNode, multicastSocket);
            multicastDiscoveryReceiver = new MulticastDiscoveryReceiver(multicastSocket);
            if (discoveryNode != null) {
                isClient = false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    @Override
    public void start() {
        initializeMulticastSocket();
        if (!isClient) {
            t = new Thread(multicastDiscoverySender);
            t.start();
        }
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        DiscoveryNode discoveryNode = null;
        MemberInfo memberInfo = multicastDiscoveryReceiver.receive();

        if (memberInfo == null) return null;
        ArrayList<DiscoveryNode> arrayList = new ArrayList<DiscoveryNode>();
        try {
            discoveryNode = new SimpleDiscoveryNode(new Address(memberInfo.getHost(), memberInfo.getPort()));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        arrayList.add(discoveryNode);
        return arrayList;
    }

    @Override
    public void destroy() {
        t.stop();
    }

    @Override
    public PartitionGroupStrategy getPartitionGroupStrategy() {
        return null;
    }

    @Override
    public Map<String, Object> discoverLocalMetadata() {
        return new HashMap<String, Object>();
    }

    private <T extends Comparable> T getOrNull(PropertyDefinition property) {
        return getOrDefault(property, null);
    }

    private <T extends Comparable> T getOrDefault(PropertyDefinition property, T defaultValue) {

        if (properties == null || property == null) {
            return defaultValue;
        }

        Comparable value = properties.get(property.key());
        if (value == null) {
            return defaultValue;
        }

        return (T) value;
    }


}
