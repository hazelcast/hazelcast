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

package com.hazelcast.aws;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.spi.partitiongroup.PartitionGroupMetaData.PARTITION_GROUP_ZONE;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class AwsDiscoveryStrategyTest
        extends HazelcastTestSupport {

    private final AWSClient mockClient = mock(AWSClient.class);
    private final AwsDiscoveryStrategy awsDiscoveryStrategy = new AwsDiscoveryStrategy(Collections.<String, Comparable>emptyMap(),
            mockClient);

    @Test
    public void discoverLocalMetadata() {
        // given
        given(mockClient.getAvailabilityZone()).willReturn("us-east-1a");

        // when
        Map<String, String> localMetaData = awsDiscoveryStrategy.discoverLocalMetadata();

        // then
        assertEquals("us-east-1a", localMetaData.get(PARTITION_GROUP_ZONE));
    }

    @Test
    public void discoverNodesNoAddresses()
            throws Exception {
        // given
        given(mockClient.getAddresses()).willReturn(Collections.<String, String>emptyMap());

        // when
        Iterable<DiscoveryNode> result = awsDiscoveryStrategy.discoverNodes();

        // then
        assertEquals(emptyList(), result);
    }

    @Test
    public void discoverNodesOneAddress()
            throws Exception {
        // given
        String privateAddress = "10.0.0.1";
        String publicAddress = "156.24.63.1";
        given(mockClient.getAddresses()).willReturn(Collections.singletonMap(privateAddress, publicAddress));

        // when
        Iterable<DiscoveryNode> result = awsDiscoveryStrategy.discoverNodes();

        // then
        int defaultPortFrom = 5701;
        int defaultPortTo = 5708;
        Iterator<DiscoveryNode> iterator = result.iterator();
        for (int port = defaultPortFrom; port <= defaultPortTo; port++) {
            DiscoveryNode node = iterator.next();
            assertEquals(new Address(privateAddress, port), node.getPrivateAddress());
            assertEquals(new Address(publicAddress, port), node.getPublicAddress());
        }
        assertFalse(iterator.hasNext());
    }

    @Test
    public void discoverNodesOneAddressOnePort()
            throws Exception {
        // given
        String privateAddress = "10.0.0.1";
        String publicAddress = "156.24.63.1";
        int port = 5701;
        given(mockClient.getAddresses()).willReturn(Collections.singletonMap(privateAddress, publicAddress));
        AwsDiscoveryStrategy awsDiscoveryStrategy = new AwsDiscoveryStrategy(
                Collections.<String, Comparable>singletonMap("hz-port", port), mockClient);

        // when
        Iterable<DiscoveryNode> result = awsDiscoveryStrategy.discoverNodes();

        // then
        Iterator<DiscoveryNode> iterator = result.iterator();
        DiscoveryNode node = iterator.next();
        assertEquals(new Address(privateAddress, port), node.getPrivateAddress());
        assertEquals(new Address(publicAddress, port), node.getPublicAddress());
        assertFalse(iterator.hasNext());
    }
}
