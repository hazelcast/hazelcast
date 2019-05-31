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

package com.hazelcast.gcp;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.partitiongroup.PartitionGroupMetaData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class GcpDiscoveryStrategyTest {
    private static final String ZONE = "us-east1-a";
    private static final int PORT1 = 5701;
    private static final int PORT2 = 5702;

    @Mock
    private GcpClient gcpClient;

    private GcpDiscoveryStrategy gcpDiscoveryStrategy;

    @Before
    public void setUp() {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("hz-port", String.format("%s-%s", PORT1, PORT2));
        gcpDiscoveryStrategy = new GcpDiscoveryStrategy(properties, gcpClient);
    }

    @Test
    public void discoverLocalMetadata() {
        // given
        given(gcpClient.getAvailabilityZone()).willReturn(ZONE);

        // when
        Map<String, String> result1 = gcpDiscoveryStrategy.discoverLocalMetadata();
        Map<String, String> result2 = gcpDiscoveryStrategy.discoverLocalMetadata();

        // then
        assertEquals(ZONE, result1.get(PartitionGroupMetaData.PARTITION_GROUP_ZONE));
        assertEquals(ZONE, result2.get(PartitionGroupMetaData.PARTITION_GROUP_ZONE));
        verify(gcpClient).getAvailabilityZone();
    }

    @Test
    public void newValidProperties() {
        // given
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("projects", "project1");
        properties.put("zones", "us-east1-b");

        // when
        new GcpDiscoveryStrategy(properties);

        // then
        // no exception
    }

    @Test(expected = InvalidConfigurationException.class)
    public void newInvalidPortRangeProperty() {
        // given
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("hz-port", "invalid");

        // when
        new GcpDiscoveryStrategy(properties);

        // then
        // throw exception
    }

    @Test(expected = InvalidConfigurationException.class)
    public void newInvalidLabelProperty() {
        // given
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("label", "invalid");

        // when
        new GcpDiscoveryStrategy(properties);

        // then
        // throw exception
    }

    @Test
    public void discoverNodes() {
        // given
        GcpAddress gcpInstance1 = new GcpAddress("192.168.1.15", "38.146.24.2");
        GcpAddress gcpInstance2 = new GcpAddress("192.168.1.16", "38.146.28.15");
        given(gcpClient.getAddresses()).willReturn(asList(gcpInstance1, gcpInstance2));

        // when
        Iterable<DiscoveryNode> nodes = gcpDiscoveryStrategy.discoverNodes();

        // then
        Iterator<DiscoveryNode> iter = nodes.iterator();

        DiscoveryNode node1 = iter.next();
        assertEquals(gcpInstance1.getPrivateAddress(), node1.getPrivateAddress().getHost());
        assertEquals(gcpInstance1.getPublicAddress(), node1.getPublicAddress().getHost());
        assertEquals(PORT1, node1.getPrivateAddress().getPort());

        DiscoveryNode node2 = iter.next();
        assertEquals(gcpInstance1.getPrivateAddress(), node2.getPrivateAddress().getHost());
        assertEquals(gcpInstance1.getPublicAddress(), node2.getPublicAddress().getHost());
        assertEquals(PORT2, node2.getPrivateAddress().getPort());

        DiscoveryNode node3 = iter.next();
        assertEquals(gcpInstance2.getPrivateAddress(), node3.getPrivateAddress().getHost());
        assertEquals(gcpInstance2.getPublicAddress(), node3.getPublicAddress().getHost());
        assertEquals(PORT1, node3.getPrivateAddress().getPort());

        DiscoveryNode node4 = iter.next();
        assertEquals(gcpInstance2.getPrivateAddress(), node4.getPrivateAddress().getHost());
        assertEquals(gcpInstance2.getPublicAddress(), node4.getPublicAddress().getHost());
        assertEquals(PORT2, node4.getPrivateAddress().getPort());
    }

    @Test
    public void discoverNodesEmpty() {
        // given
        given(gcpClient.getAddresses()).willReturn(new ArrayList<GcpAddress>());

        // when
        Iterable<DiscoveryNode> nodes = gcpDiscoveryStrategy.discoverNodes();

        // then
        assertFalse(nodes.iterator().hasNext());
    }

    @Test
    public void discoverNodesException() {
        // given
        given(gcpClient.getAddresses()).willThrow(new RuntimeException("Error while checking GCP instances"));

        // when
        Iterable<DiscoveryNode> nodes = gcpDiscoveryStrategy.discoverNodes();

        // then
        assertFalse(nodes.iterator().hasNext());
    }
}