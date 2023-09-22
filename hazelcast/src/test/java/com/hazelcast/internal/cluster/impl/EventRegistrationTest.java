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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
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

import static com.hazelcast.spi.properties.ClusterProperty.ASYNC_JOIN_STRATEGY_ENABLED;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.TestEnvironment.HAZELCAST_TEST_USE_NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class EventRegistrationTest extends HazelcastTestSupport {

    @Rule
    public final OverridePropertyRule overridePropertyRule = set(HAZELCAST_TEST_USE_NETWORK, "true");
    @Rule
    public final OverridePropertyRule overridePropertyRule2 = set(ASYNC_JOIN_STRATEGY_ENABLED.getName(), "false");
    private final HazelcastInstance[] batchedMembers = new HazelcastInstance[6];

    @Before
    public void prepare() throws InterruptedException {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        // Create a cluster that goes through a batched join, starting with a master instance (always index 0)
        batchedMembers[0] = factory.newHazelcastInstance(config);
        // Start 3 members simultaneously (so their join requests are batched)
        List<Future<?>> futures = new ArrayList<>(3);
        for (int k = 1; k < 4; k++) {
            int finalK = k;
            futures.add(spawn(() -> batchedMembers[finalK] = factory.newHazelcastInstance(config)));
        }

        // Wait for all batched member instances to be created
        FutureUtil.waitWithDeadline(futures, 30, TimeUnit.SECONDS);

        // Ensure the batched join period has elapsed
        ClusterJoinManager joinManager = ((ClusterServiceImpl) getNodeEngineImpl(batchedMembers[0])
                .getClusterService()).getClusterJoinManager();
        assertTrueEventually(() -> assertFalse(joinManager.isBatchingJoins(Clock.currentTimeMillis())));

        // Then start 2 members sequentially (so their join requests are not batched)
        for (int k = 4; k < 6; k++) {
            batchedMembers[k] = factory.newHazelcastInstance(config);
        }

        // Ensure cluster size is 6
        assertClusterSizeEventually(6, batchedMembers);
    }

    /**
     * <a href="https://hazelcast.atlassian.net/browse/HZ-2797">Related to HZ-2797</a>
     */
    @Test
    public void testEventRegistrationsForAllMembers_AfterJoin() {
        // Gather a set of expected addresses to use in comparisons
        Set<Address> expectedAddresses = new HashSet<>(batchedMembers.length);
        for (HazelcastInstance member : batchedMembers) {
            expectedAddresses.add(getNodeEngineImpl(member).getThisAddress());
        }

        // Iterate over all members of the cluster and assert that it has listeners registered for
        //  every member of the cluster (including the local member itself)
        Set<Address> localAddresses = new HashSet<>(batchedMembers.length);
        for (HazelcastInstance member : batchedMembers) {
            Collection<EventRegistration> registrations = getNodeEngineImpl(member).getEventService().getRegistrations(
                    ProxyServiceImpl.SERVICE_NAME, ProxyServiceImpl.SERVICE_NAME);

            localAddresses.clear();
            registrations.forEach(registration -> localAddresses.add(registration.getSubscriber()));

            assertEquals(String.format("Expected: %s, Actual: %s", expectedAddresses, localAddresses),
                    expectedAddresses.size(), localAddresses.size());
            assertContainsAll(localAddresses, expectedAddresses);
        }
    }
}
