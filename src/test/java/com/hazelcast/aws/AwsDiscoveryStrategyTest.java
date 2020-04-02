/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.aws;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.spi.discovery.DiscoveryNode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.spi.partitiongroup.PartitionGroupMetaData.PARTITION_GROUP_ZONE;
import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;

@RunWith(MockitoJUnitRunner.class)
public class AwsDiscoveryStrategyTest {
    private static final int PORT1 = 5701;
    private static final int PORT2 = 5702;
    private static final String ZONE = "us-east-1a";

    @Mock
    private AwsClient awsClient;

    private AwsDiscoveryStrategy awsDiscoveryStrategy;

    @Before
    public void setUp() {
        Map<String, Comparable> properties = new HashMap<>();
        properties.put("hz-port", String.format("%s-%s", PORT1, PORT2));
        awsDiscoveryStrategy = new AwsDiscoveryStrategy(properties, awsClient);
    }

    private Map<String, Comparable> getProperties() {
        Map<String, Comparable> properties = new HashMap<>();
        return properties;
    }

    @Test(expected = InvalidConfigurationException.class)
    public void newInvalidPortRangeProperty() {
        // given
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("hz-port", "invalid");

        // when
        new AwsDiscoveryStrategy(properties);

        // then
        // throw exception
    }

    @Test
    public void discoverLocalMetadata() {
        // given
        given(awsClient.getAvailabilityZone()).willReturn(ZONE);

        // when
        Map<String, String> localMetaData = awsDiscoveryStrategy.discoverLocalMetadata();

        // then
        assertEquals(ZONE, localMetaData.get(PARTITION_GROUP_ZONE));
    }

    @Test
    public void discoverNodes() throws IOException {
        // given
        String privateIp = "192.168.1.15";
        String publicIp = "38.146.24.2";
        given(awsClient.getAddresses()).willReturn(ImmutableMap.of(privateIp, publicIp));

        // when
        Iterable<DiscoveryNode> nodes = awsDiscoveryStrategy.discoverNodes();

        // then
        List<DiscoveryNode> nodeList = toList(nodes);
        DiscoveryNode node1 = nodeList.get(0);
        assertEquals(privateIp, node1.getPrivateAddress().getHost());
        assertEquals(PORT1, node1.getPrivateAddress().getPort());
        assertEquals(publicIp, node1.getPublicAddress().getHost());

        DiscoveryNode node2 = nodeList.get(1);
        assertEquals(privateIp, node2.getPrivateAddress().getHost());
        assertEquals(PORT2, node2.getPrivateAddress().getPort());
        assertEquals(publicIp, node2.getPublicAddress().getHost());
    }

    @Test
    public void discoverNodesMultipleAddressesManyPorts() throws IOException {
        // given
        // 8 ports in the port range
        Map<String, Comparable> properties = new HashMap<>();
        properties.put("hz-port", "5701-5708");
        awsDiscoveryStrategy = new AwsDiscoveryStrategy(properties, awsClient);

        // 2 instances found
        given(awsClient.getAddresses()).willReturn(ImmutableMap.of(
            "192.168.1.15", "38.146.24.2",
            "192.168.1.16", "38.146.28.15"
        ));

        // when
        Iterable<DiscoveryNode> nodes = awsDiscoveryStrategy.discoverNodes();

        // then
        // 2 * 8 = 16 addresses found
        assertThat(toList(nodes), hasSize(16));
    }

    @Test
    public void discoverNodesEmpty() throws IOException {
        // given
        given(awsClient.getAddresses()).willReturn(Collections.emptyMap());

        // when
        Iterable<DiscoveryNode> result = awsDiscoveryStrategy.discoverNodes();

        // then
        assertEquals(emptyList(), result);
    }

    @Test
    public void discoverNodesException() throws IOException {
        // given
        given(awsClient.getAddresses()).willThrow(new RuntimeException("Unknown exception"));

        // when
        Iterable<DiscoveryNode> result = awsDiscoveryStrategy.discoverNodes();

        // then
        assertEquals(emptyList(), result);
    }

    private static List<DiscoveryNode> toList(Iterable<DiscoveryNode> nodes) {
        List<DiscoveryNode> list = new ArrayList<>();
        nodes.forEach(list::add);
        return list;
    }
}
