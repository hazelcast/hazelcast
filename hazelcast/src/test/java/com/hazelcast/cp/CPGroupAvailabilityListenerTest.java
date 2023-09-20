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

package com.hazelcast.cp;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.event.CPGroupAvailabilityEvent;
import com.hazelcast.cp.event.CPGroupAvailabilityListener;
import com.hazelcast.cp.event.impl.CPGroupAvailabilityEventGracefulImpl;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

    static class GracefulShutdownAvailabilityListener
            implements CPGroupAvailabilityListener {
        private final AtomicInteger handlerHits;

        GracefulShutdownAvailabilityListener() {
            handlerHits = new AtomicInteger();
        }

        @Override
        public void availabilityDecreased(CPGroupAvailabilityEvent event) {
            if (event instanceof CPGroupAvailabilityEventGracefulImpl) {
                handlerHits.incrementAndGet();
            }
        }

        // majority is completely broken in this scenario
        @Override
        public void majorityLost(CPGroupAvailabilityEvent event) {
            if (event instanceof CPGroupAvailabilityEventGracefulImpl) {
                handlerHits.incrementAndGet();
            }
        }
    }

    @Test
    public void whenMemberShutdown_thenReceiveEvents3Graceful() {
        GracefulShutdownAvailabilityListener listener = new GracefulShutdownAvailabilityListener();

        HazelcastInstance[] instances = newInstances(3);
        HazelcastInstance member1 = instances[0];
        HazelcastInstance member2 = instances[1];
        HazelcastInstance member3 = instances[2];
        member2.getCPSubsystem().addGroupAvailabilityListener(listener);
        member3.getCPSubsystem().addGroupAvailabilityListener(listener);

        int expectedAvailabilityEvents = 2;
        member1.getLifecycleService().shutdown();
        assertEqualsEventually(expectedAvailabilityEvents, listener.handlerHits);

        expectedAvailabilityEvents += 1;
        member2.getLifecycleService().shutdown();
        // majority lost ---
        assertEqualsEventually(expectedAvailabilityEvents, listener.handlerHits);
    }

    @Test
    public void whenMemberShutdown_thenReceiveEvents5Graceful() {
        GracefulShutdownAvailabilityListener listener = new GracefulShutdownAvailabilityListener();

        HazelcastInstance[] instances = newInstances(5);
        instances[1].getCPSubsystem().addGroupAvailabilityListener(listener);
        instances[2].getCPSubsystem().addGroupAvailabilityListener(listener);
        instances[3].getCPSubsystem().addGroupAvailabilityListener(listener);
        instances[4].getCPSubsystem().addGroupAvailabilityListener(listener);

        int expectedAvailabilityEvents = 4;
        instances[0].getLifecycleService().shutdown();
        assertEqualsEventually(expectedAvailabilityEvents, listener.handlerHits);

        instances[1].getLifecycleService().shutdown();
        int remainingMembers = 3;
        expectedAvailabilityEvents += remainingMembers;
        assertEqualsEventually(expectedAvailabilityEvents, listener.handlerHits);

        instances[2].getLifecycleService().shutdown();
        // majority should be lost here surely...
        remainingMembers = 2;
        expectedAvailabilityEvents += remainingMembers;
        assertEqualsEventually(expectedAvailabilityEvents, listener.handlerHits);

        instances[3].getLifecycleService().shutdown();
        remainingMembers = 1;
        expectedAvailabilityEvents += remainingMembers;
        assertEqualsEventually(expectedAvailabilityEvents, listener.handlerHits);
    }
}
