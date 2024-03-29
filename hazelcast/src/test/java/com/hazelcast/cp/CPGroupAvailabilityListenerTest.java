/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.event.CPGroupAvailabilityEvent;
import com.hazelcast.cp.event.CPGroupAvailabilityListener;
import com.hazelcast.cp.event.impl.CPGroupAvailabilityEventImpl;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPGroupAvailabilityListenerTest extends HazelcastRaftTestSupport {

    @Test
    public void whenRegisteredInConfig_thenReceiveEvents() {
        CountingDownCPGroupAvailabilityListener listener = new CountingDownCPGroupAvailabilityListener(1, 1);
        int cpCount = 3;
        int groupSize = 3;

        Config config = createConfig(cpCount, groupSize);
        config.addListenerConfig(new ListenerConfig().setImplementation(listener));

        HazelcastInstance[] instances = factory.newInstances(config, cpCount);
        waitUntilCPDiscoveryCompleted(instances);

        instances[2].getLifecycleService().terminate();
        assertOpenEventually(listener.availabilityLatch);
    }

    @Test
    public void whenMemberTerminated_thenReceiveEvents() throws Exception {
        CountingDownCPGroupAvailabilityListener listener = new CountingDownCPGroupAvailabilityListener(1, 1);

        HazelcastInstance[] instances = newInstances(5);
        instances[1].getCPSubsystem().addGroupAvailabilityListener(listener);

        instances[0].getLifecycleService().terminate();
        assertOpenEventually(listener.availabilityLatch);
        assertEquals(1, listener.availabilityEventCount.get());

        instances[4].getLifecycleService().terminate();
        assertFalse(listener.majorityLatch.await(1, TimeUnit.SECONDS));
        assertEquals(2, listener.availabilityEventCount.get());

        instances[2].getLifecycleService().terminate();
        assertOpenEventually(listener.majorityLatch);
        assertEquals(1, listener.majorityEventCount.get());
    }

    @Test
    public void whenListenerDeregistered_thenNoEventsReceived() {
        CountingDownCPGroupAvailabilityListener listener = new CountingDownCPGroupAvailabilityListener(1, 1);

        HazelcastInstance[] instances = newInstances(3);
        UUID id = instances[1].getCPSubsystem().addGroupAvailabilityListener(listener);

        instances[0].getLifecycleService().terminate();
        assertEqualsEventually(1, listener.availabilityEventCount);

        assertTrue(instances[1].getCPSubsystem().removeGroupAvailabilityListener(id));

        instances[2].getLifecycleService().terminate();
        assertTrueAllTheTime(() -> assertEquals(0, listener.majorityEventCount.get()), 3);
    }

    @Test
    public void whenMultipleListenersRegistered_thenAllReceiveEvents() {
        HazelcastInstance[] instances = newInstances(3);

        CountingDownCPGroupAvailabilityListener listener1 = new CountingDownCPGroupAvailabilityListener(1, 1);
        instances[1].getCPSubsystem().addGroupAvailabilityListener(listener1);
        CountingDownCPGroupAvailabilityListener listener2 = new CountingDownCPGroupAvailabilityListener(1, 1);
        instances[1].getCPSubsystem().addGroupAvailabilityListener(listener2);

        instances[0].getLifecycleService().terminate();
        assertOpenEventually(listener1.availabilityLatch);
        assertOpenEventually(listener2.availabilityLatch);
        assertEquals(1, listener1.availabilityEventCount.get());
        assertEquals(1, listener2.availabilityEventCount.get());
    }

    public static class CountingDownCPGroupAvailabilityListener implements CPGroupAvailabilityListener {
        public final CountDownLatch availabilityLatch;
        public final CountDownLatch majorityLatch;
        public final AtomicInteger availabilityEventCount = new AtomicInteger();
        public final AtomicInteger majorityEventCount = new AtomicInteger();

        public CountingDownCPGroupAvailabilityListener(int groupAvailability, int groupMajority) {
            this.availabilityLatch = new CountDownLatch(groupAvailability);
            this.majorityLatch = new CountDownLatch(groupMajority);
        }

        @Override
        public void availabilityDecreased(CPGroupAvailabilityEvent event) {
            assertTrue("Event: " + event, event.isMajorityAvailable());
            availabilityEventCount.incrementAndGet();
            availabilityLatch.countDown();
        }

        @Override
        public void majorityLost(CPGroupAvailabilityEvent event) {
            assertFalse("Event: " + event, event.isMajorityAvailable());
            majorityEventCount.incrementAndGet();
            majorityLatch.countDown();
        }
    }

    public static class GracefulShutdownAvailabilityListener
            implements CPGroupAvailabilityListener {
        public final AtomicInteger availabilityDecreased;
        private final AtomicInteger majorityLost;

        private final Set<UUID> membersShutdown;

        public GracefulShutdownAvailabilityListener() {
            availabilityDecreased = new AtomicInteger();
            majorityLost = new AtomicInteger();
            membersShutdown = ConcurrentHashMap.newKeySet();
        }

        @Override
        public void availabilityDecreased(CPGroupAvailabilityEvent event) {
            handleEvent(event, true);
        }

        @Override
        public void majorityLost(CPGroupAvailabilityEvent event) {
            handleEvent(event, false);
        }

        private void handleEvent(CPGroupAvailabilityEvent event, boolean isAvailabilityDecrease) {
            CPGroupAvailabilityEventImpl impl = (CPGroupAvailabilityEventImpl) event;
            if (impl.isShutdown()) {
                membersShutdown.addAll(event.getUnavailableMembers().stream().map(CPMember::getUuid).collect(Collectors.toSet()));
                if (isAvailabilityDecrease) {
                    availabilityDecreased.incrementAndGet();
                } else {
                    majorityLost.incrementAndGet();
                }
            }
        }
    }

    @Test
    public void whenMemberShutdown_thenReceiveGracefulAvailabilityEvents_3_cpNodeCount() {
        whenMemberShutdown_thenReceiveGracefulAvailabilityEvents(3);
    }

    @Test
    public void whenMemberShutdown_thenReceiveGracefulAvailabilityEvents_5_cpNodeCount() {
        whenMemberShutdown_thenReceiveGracefulAvailabilityEvents(5);
    }

    public void whenMemberShutdown_thenReceiveGracefulAvailabilityEvents(int cpNodeCount) {
        GracefulShutdownAvailabilityListener listener = new GracefulShutdownAvailabilityListener();

        HazelcastInstance[] instances = newInstances(cpNodeCount);
        // instances[0] is the only instance we won't shut down; it will observe all the applicable availability events
        instances[0].getCPSubsystem().addGroupAvailabilityListener(listener);

        HashSet<UUID> expectedShutdownInstanceUuids = new HashSet<>();
        for (int i = 1; i < instances.length; i++) {
            expectedShutdownInstanceUuids.add(instances[i].getLocalEndpoint().getUuid());
        }

        // shutdown the other members until we have only two instances left:
        //   - instances[0]                   : the one with our event handler attached
        //   - instances[instances.length - 1]: we'll shut down his one separately as it will result in loss of majority
        for (int i = 1; i < instances.length - 1; i++) {
            instances[i].getLifecycleService().shutdown();
            assertEqualsEventually(i, listener.availabilityDecreased);
        }

        instances[instances.length - 1].getLifecycleService().shutdown();
        assertEqualsEventually(1, listener.majorityLost);

        assertEquals(expectedShutdownInstanceUuids, listener.membersShutdown);
    }

    @Test
    public void whenMemberExitsNormally_thenReceiveEventsCommunityTest() throws Exception {
        GracefulShutdownAvailabilityListener listener = new GracefulShutdownAvailabilityListener();

        HazelcastInstance[] instances = newInstances(3);
        instances[1].getCPSubsystem().addGroupAvailabilityListener(listener);

        instances[0].shutdown();
        assertEqualsEventually(1, listener.availabilityDecreased);

        instances[2].shutdown();
        assertEqualsEventually(1, listener.majorityLost);
        assertEquals(1, listener.availabilityDecreased.get());
    }
}
