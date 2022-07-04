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

package com.hazelcast.azure;

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
public class AzureDiscoveryStrategyTest {
    private static final String ZONE = "us-east1-a";
    private static final int PORT1 = 5701;
    private static final int PORT2 = 5702;

    @Mock
    private AzureClient azureClient;

    private AzureDiscoveryStrategy azureDiscoveryStrategy;

    @Before
    public void setUp() {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("hz-port", String.format("%s-%s", PORT1, PORT2));
        azureDiscoveryStrategy = new AzureDiscoveryStrategy(properties, azureClient);
    }

    @Test
    public void discoverLocalMetadata() {
        // given
        given(azureClient.getAvailabilityZone()).willReturn(ZONE);

        // when
        Map<String, String> result1 = azureDiscoveryStrategy.discoverLocalMetadata();
        Map<String, String> result2 = azureDiscoveryStrategy.discoverLocalMetadata();

        // then
        assertEquals(ZONE, result1.get(PartitionGroupMetaData.PARTITION_GROUP_ZONE));
        assertEquals(ZONE, result2.get(PartitionGroupMetaData.PARTITION_GROUP_ZONE));
        verify(azureClient).getAvailabilityZone();
    }

    @Test
    public void newValidProperties() {
        // given
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("tenant-id", "subscription-id-1");
        properties.put("client-id", "subscription-id-1");
        properties.put("client-secret", "subscription-id-1");
        properties.put("subscription-id", "subscription-id-1");
        properties.put("resource-group", "resource-group-1");
        properties.put("scale-set", "scale-set-1");
        properties.put("tag", "tag-1=value-1");
        properties.put("instance-metadata-available", Boolean.FALSE);

        // when
        new AzureDiscoveryStrategy(properties);

        // then
        // no exception
    }

    @Test(expected = InvalidConfigurationException.class)
    public void newInvalidPortRangeProperty() {
        // given
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("hz-port", "invalid");

        // when
        new AzureDiscoveryStrategy(properties);

        // then
        // throw exception
    }

    @Test
    public void discoverNodes() {
        // given
        AzureAddress azureAddress1 = new AzureAddress("192.168.1.15", "38.146.24.2");
        AzureAddress azureAddress2 = new AzureAddress("192.168.1.16", "38.146.28.15");
        given(azureClient.getAddresses()).willReturn(asList(azureAddress1, azureAddress2));

        // when
        Iterable<DiscoveryNode> nodes = azureDiscoveryStrategy.discoverNodes();

        // then
        Iterator<DiscoveryNode> iter = nodes.iterator();

        DiscoveryNode node1 = iter.next();
        assertEquals(azureAddress1.getPrivateAddress(), node1.getPrivateAddress().getHost());
        assertEquals(azureAddress1.getPublicAddress(), node1.getPublicAddress().getHost());
        assertEquals(PORT1, node1.getPrivateAddress().getPort());

        DiscoveryNode node2 = iter.next();
        assertEquals(azureAddress1.getPrivateAddress(), node2.getPrivateAddress().getHost());
        assertEquals(azureAddress1.getPublicAddress(), node2.getPublicAddress().getHost());
        assertEquals(PORT2, node2.getPrivateAddress().getPort());

        DiscoveryNode node3 = iter.next();
        assertEquals(azureAddress2.getPrivateAddress(), node3.getPrivateAddress().getHost());
        assertEquals(azureAddress2.getPublicAddress(), node3.getPublicAddress().getHost());
        assertEquals(PORT1, node3.getPrivateAddress().getPort());

        DiscoveryNode node4 = iter.next();
        assertEquals(azureAddress2.getPrivateAddress(), node4.getPrivateAddress().getHost());
        assertEquals(azureAddress2.getPublicAddress(), node4.getPublicAddress().getHost());
        assertEquals(PORT2, node4.getPrivateAddress().getPort());
    }

    @Test
    public void discoverNodesEmpty() {
        // given
        given(azureClient.getAddresses()).willReturn(new ArrayList<AzureAddress>());

        // when
        Iterable<DiscoveryNode> nodes = azureDiscoveryStrategy.discoverNodes();

        // then
        assertFalse(nodes.iterator().hasNext());
    }

    @Test
    public void discoverNodesException() {
        // given
        given(azureClient.getAddresses()).willThrow(new RuntimeException("Error while checking Azure instances"));

        // when
        Iterable<DiscoveryNode> nodes = azureDiscoveryStrategy.discoverNodes();

        // then
        assertFalse(nodes.iterator().hasNext());
    }
}
