/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class ClusterBatchedJoinTest extends HazelcastTestSupport {

    private final HazelcastInstance[] batchedMembers = new HazelcastInstance[10];

    @Before
    public void prepare() throws InterruptedException {
        // Prepare members list and config
        List<String> membersList = new ArrayList<>(10);
        for (int k = 0; k < 10; k++) {
            membersList.add("127.0.0.1:" + (5701 + k));
        }
        Config config = smallInstanceConfigWithoutJetAndMetrics().setClusterName("myCluster");
        config.getNetworkConfig().setPort(5701);
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getTcpIpConfig().setEnabled(true).setMembers(membersList);

        // Create a cluster that goes through a batched join, starting with a master instance (always index 0)
        batchedMembers[0] = Hazelcast.newHazelcastInstance(config);
        // Start 9 members simultaneously (so their join request is batched)
        List<Future<?>> futures = new ArrayList<>();
        for (int k = 1; k < 10; k++) {
            int finalK = k;
            futures.add(spawn(() -> batchedMembers[finalK] = Hazelcast.newHazelcastInstance(config)));
        }

        // Wait for all member instances to be created
        FutureUtil.waitWithDeadline(futures, 30, TimeUnit.SECONDS);

        // Ensure cluster size is 10
        assertClusterSizeEventually(10, batchedMembers);
    }

    @After
    public void cleanUp() {
        // Terminate cluster members
        for (HazelcastInstance member : batchedMembers) {
            member.getLifecycleService().terminate();
        }
    }

    /**
     * <a href="https://hazelcast.atlassian.net/browse/HZ-2797">Related to HZ-2797</a>
     */
    @Test
    public void testProxyServiceRegistrationsPresentForAllMembers() {
        // Gather a set of expected addresses to use in comparisons
        Set<Address> expectedAddresses = new HashSet<>(batchedMembers.length);
        for (HazelcastInstance member : batchedMembers) {
            expectedAddresses.add(getNodeEngineImpl(member).getThisAddress());
        }

        // Iterate over all members of the cluster and assert that it has listeners registered for
        //  every member of the cluster (including the local member itself)
        Set<Address> localAddresses = new HashSet<>(batchedMembers.length);
        for (int i = 0; i < 10; i++) {
            HazelcastInstance member = batchedMembers[i];
            Collection<EventRegistration> registrations = getNodeEngineImpl(member)
                    .getEventService()
                    .getRegistrations(ProxyServiceImpl.SERVICE_NAME, ProxyServiceImpl.SERVICE_NAME);

            localAddresses.clear();
            registrations.forEach(registration -> localAddresses.add(registration.getSubscriber()));

            assertEquals(expectedAddresses.size(), localAddresses.size());
            assertContainsAll(localAddresses, expectedAddresses);
        }
    }
}
