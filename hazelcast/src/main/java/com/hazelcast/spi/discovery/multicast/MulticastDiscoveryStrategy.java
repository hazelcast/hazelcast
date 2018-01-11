/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.discovery.multicast;

import com.hazelcast.config.properties.ValidationException;
import com.hazelcast.config.properties.ValueValidator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.spi.discovery.multicast.impl.MulticastDiscoveryReceiver;
import com.hazelcast.spi.discovery.multicast.impl.MulticastDiscoverySender;
import com.hazelcast.spi.discovery.multicast.impl.MulticastMemberInfo;
import com.hazelcast.spi.partitiongroup.PartitionGroupStrategy;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * The multicast {@link com.hazelcast.spi.discovery.DiscoveryStrategy}.
 */
public class MulticastDiscoveryStrategy extends AbstractDiscoveryStrategy {

    private static final int DATA_OUTPUT_BUFFER_SIZE = 64 * 1024;
    private static final int DEFAULT_MULTICAST_PORT = 54327;
    private static final int SOCKET_TIME_TO_LIVE = 255;
    private static final int SOCKET_TIMEOUT = 3000;
    private static final String DEFAULT_MULTICAST_GROUP = "224.2.2.3";

    private DiscoveryNode discoveryNode;
    private MulticastSocket multicastSocket;
    private Thread thread;
    private MulticastDiscoveryReceiver multicastDiscoveryReceiver;
    private MulticastDiscoverySender multicastDiscoverySender;
    private ILogger logger;
    private boolean isClient;

    public MulticastDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger, Map<String, Comparable> properties) {
        super(logger, properties);
        this.discoveryNode = discoveryNode;
        this.logger = logger;
    }

    private void initializeMulticastSocket() {
        try {
            int port = getOrDefault(MulticastProperties.PORT, DEFAULT_MULTICAST_PORT);
            PortValueValidator validator = new PortValueValidator();
            validator.validate(port);
            String group = getOrDefault(MulticastProperties.GROUP, DEFAULT_MULTICAST_GROUP);
            multicastSocket = new MulticastSocket(null);
            multicastSocket.bind(new InetSocketAddress(port));
            multicastSocket.setReuseAddress(true);
            multicastSocket.setTimeToLive(SOCKET_TIME_TO_LIVE);
            multicastSocket.setReceiveBufferSize(DATA_OUTPUT_BUFFER_SIZE);
            multicastSocket.setSendBufferSize(DATA_OUTPUT_BUFFER_SIZE);
            multicastSocket.setSoTimeout(SOCKET_TIMEOUT);
            multicastSocket.joinGroup(InetAddress.getByName(group));
            multicastDiscoverySender = new MulticastDiscoverySender(discoveryNode, multicastSocket, logger, group, port);
            multicastDiscoveryReceiver = new MulticastDiscoveryReceiver(multicastSocket, logger);
            if (discoveryNode == null) {
                isClient = true;
            }
        } catch (Exception e) {
            logger.finest(e.getMessage());
            rethrow(e);
        }
    }

    @Override
    public void start() {
        initializeMulticastSocket();
        if (!isClient) {
            thread = new Thread(multicastDiscoverySender);
            thread.start();
        }
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        DiscoveryNode discoveryNode;
        MulticastMemberInfo multicastMemberInfo = multicastDiscoveryReceiver.receive();

        if (multicastMemberInfo == null) {
            return null;
        }
        ArrayList<DiscoveryNode> arrayList = new ArrayList<DiscoveryNode>();
        try {
            discoveryNode = new SimpleDiscoveryNode(new Address(multicastMemberInfo.getHost(), multicastMemberInfo.getPort()));
            arrayList.add(discoveryNode);
        } catch (UnknownHostException e) {
            logger.finest(e.getMessage());
        }
        return arrayList;
    }

    @Override
    public void destroy() {
        multicastDiscoverySender.stop();
        if (thread != null) {
            thread.interrupt();
        }
    }

    @Override
    public PartitionGroupStrategy getPartitionGroupStrategy() {
        return null;
    }

    @Override
    public Map<String, Object> discoverLocalMetadata() {
        return new HashMap<String, Object>();
    }

    /**
     * Validator for valid network ports
     */
    private static class PortValueValidator implements ValueValidator<Integer> {
        private static final int MIN_PORT = 0;
        private static final int MAX_PORT = 65535;

        public void validate(Integer value) throws ValidationException {
            if (value < MIN_PORT) {
                throw new ValidationException("hz-port number must be greater 0");
            }
            if (value > MAX_PORT) {
                throw new ValidationException("hz-port number must be less or equal to 65535");
            }
        }
    }
}
