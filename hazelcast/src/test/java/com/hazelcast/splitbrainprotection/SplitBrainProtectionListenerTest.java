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

package com.hazelcast.splitbrainprotection;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.config.SplitBrainProtectionListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SplitBrainProtectionListenerTest extends HazelcastTestSupport {

    @Test
    public void splitBrainProtectionEventsNotFired_duringStartup() {
        EventCountingSplitBrainProtectionListener listener = new EventCountingSplitBrainProtectionListener(0, 0);
        Config config = configWithSplitBrainProtection(randomName(), listener);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        // No events yet, since minimum cluster size is not met.
        assertTrueAllTheTime(() -> assertEquals(0, listener.notPresentCount.get()), 3);

        factory.newHazelcastInstance(config);

        // No events, since this is the first time minimum cluster size is met.
        assertTrueAllTheTime(() -> assertEquals(0, listener.presentCount.get()), 3);
    }

    @Test
    public void splitBrainProtectionFailureEventFired_whenNodeCountDropsBelowThreshold() {
        EventCountingSplitBrainProtectionListener listener = new EventCountingSplitBrainProtectionListener(0, 2);
        Config config = configWithSplitBrainProtection(randomName(), listener);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance().shutdown();

        assertOpenEventually(listener.notPresentLatch);
    }

    @Test
    public void splitBrainProtectionEventsFired_whenNodeCountDropsBelowThenAboveThreshold() {
        EventCountingSplitBrainProtectionListener listener = new EventCountingSplitBrainProtectionListener(2, 2);
        Config config = configWithSplitBrainProtection(randomName(), listener);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config).shutdown();

        assertOpenEventually(listener.notPresentLatch);

        // No events on the latest member, since this is the first time min cluster size is met for it.
        factory.newHazelcastInstance(config);
        assertOpenEventually(listener.presentLatch);

        assertEquals(2, listener.notPresentCount.get());
        assertEquals(2, listener.presentCount.get());
    }

    @Test
    public void whenDifferentSplitBrainProtectionsDefined_allGetEvents() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        EventCountingSplitBrainProtectionListener listener1 = new EventCountingSplitBrainProtectionListener(0, 1);
        EventCountingSplitBrainProtectionListener listener2 = new EventCountingSplitBrainProtectionListener(0, 1);

        Config config1 = configWithSplitBrainProtection(randomName(), listener1);
        Config config2 = configWithSplitBrainProtection(randomName(), listener2);

        factory.newHazelcastInstance(config1);
        factory.newHazelcastInstance(config2);
        factory.newHazelcastInstance().shutdown();

        assertOpenEventually(listener1.notPresentLatch);
        assertEquals(1, listener1.notPresentCount.get());

        assertOpenEventually(listener2.notPresentLatch);
        assertEquals(1, listener2.notPresentCount.get());
    }

    @Test
    public void whenCustomResolverDefined_splitBrainProtectionEventsFired() {
        EventCountingSplitBrainProtectionListener listener = new EventCountingSplitBrainProtectionListener(0, 1);
        SplitBrainProtectionListenerConfig listenerConfig = new SplitBrainProtectionListenerConfig(listener);

        SplitBrainProtectionFunction function = members -> members.size() >= 2;

        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig()
                .setName(randomName())
                .setEnabled(true)
                .addListenerConfig(listenerConfig)
                .setFunctionImplementation(function);

        Config config = new Config().addSplitBrainProtectionConfig(splitBrainProtectionConfig);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config).shutdown();

        assertOpenEventually(listener.notPresentLatch);
    }

    @Test
    public void splitBrainProtectionEventShowsCorrectMemberList() {
        CountDownLatch notPresentLatch = new CountDownLatch(1);
        AtomicReference<Collection<Member>> eventMembersRef = new AtomicReference<>();

        SplitBrainProtectionListener listener = event -> {
            if (!event.isPresent()) {
                assertEquals(3, event.getThreshold());
                Collection<Member> currentMembers = event.getCurrentMembers();
                eventMembersRef.set(currentMembers);
                notPresentLatch.countDown();
            }
        };

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);

        HazelcastInstance hz1 = factory.newHazelcastInstance(configWithSplitBrainProtection(randomName(), listener));
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        factory.newHazelcastInstance().shutdown();

        assertOpenEventually(notPresentLatch);
        assertNotNull(eventMembersRef.get());

        Collection<Member> members = eventMembersRef.get();
        assertThat(members, Matchers.contains(hz1.getCluster().getLocalMember(), hz2.getCluster().getLocalMember()));
    }

    private Config configWithSplitBrainProtection(String name, SplitBrainProtectionListener listener) {
        SplitBrainProtectionListenerConfig listenerConfig = new SplitBrainProtectionListenerConfig();
        listenerConfig.setImplementation(listener);
        Config config = new Config();
        config.addSplitBrainProtectionConfig(new SplitBrainProtectionConfig(name, true, 3).addListenerConfig(listenerConfig));
        return config;
    }

    private static class EventCountingSplitBrainProtectionListener implements SplitBrainProtectionListener {
        private final AtomicInteger presentCount = new AtomicInteger();
        private final AtomicInteger notPresentCount = new AtomicInteger();
        private final CountDownLatch presentLatch;
        private final CountDownLatch notPresentLatch;

        EventCountingSplitBrainProtectionListener(int presentLatchCount, int notPresentLatchCount) {
            this.presentLatch = new CountDownLatch(presentLatchCount);
            this.notPresentLatch = new CountDownLatch(notPresentLatchCount);
        }

        @Override
        public void onChange(SplitBrainProtectionEvent event) {
            if (event.isPresent()) {
                presentCount.incrementAndGet();
                presentLatch.countDown();
            } else {
                notPresentCount.incrementAndGet();
                notPresentLatch.countDown();
            }
        }
    }
}
