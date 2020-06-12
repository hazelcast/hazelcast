/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.quorum;

import com.hazelcast.config.Config;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.QuorumListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
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
@Category({QuickTest.class, ParallelTest.class})
public class QuorumListenerTest extends HazelcastTestSupport {

    @Test
    public void quorumEventsNotFired_duringStartup() {
        final EventCountingQuorumListener listener = new EventCountingQuorumListener(0, 0);
        Config config = configWithQuorum(randomName(), listener);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        // No events yet, since quorum is not met.
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, listener.quorumNotPresentCount.get());
            }
        }, 3);

        factory.newHazelcastInstance(config);

        // No events, since this is the first time quorum is met.
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, listener.quorumPresentCount.get());
            }
        }, 3);
    }

    @Test
    public void quorumFailureEventFired_whenNodeCountDropsBelowThreshold() {
        EventCountingQuorumListener listener = new EventCountingQuorumListener(0, 2);
        Config config = configWithQuorum(randomName(), listener);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance().shutdown();

        assertOpenEventually(listener.quorumNotPresent);
    }

    @Test
    public void quorumEventsFired_whenNodeCountDropsBelowThenAboveThreshold() {
        EventCountingQuorumListener listener = new EventCountingQuorumListener(2, 2);
        Config config = configWithQuorum(randomName(), listener);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config).shutdown();

        assertOpenEventually(listener.quorumNotPresent);

        // No events on the latest member, since this is the first time quorum is met for it.
        factory.newHazelcastInstance(config);
        assertOpenEventually(listener.quorumPresent);

        assertEquals(2, listener.quorumNotPresentCount.get());
        assertEquals(2, listener.quorumPresentCount.get());
    }

    @Test
    public void whenDifferentQuorumsDefined_allGetEvents() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        EventCountingQuorumListener listener1 = new EventCountingQuorumListener(0, 1);
        EventCountingQuorumListener listener2 = new EventCountingQuorumListener(0, 1);

        Config config1 = configWithQuorum(randomName(), listener1);
        Config config2 = configWithQuorum(randomName(), listener2);

        factory.newHazelcastInstance(config1);
        factory.newHazelcastInstance(config2);
        factory.newHazelcastInstance().shutdown();

        assertOpenEventually(listener1.quorumNotPresent);
        assertEquals(1, listener1.quorumNotPresentCount.get());

        assertOpenEventually(listener2.quorumNotPresent);
        assertEquals(1, listener2.quorumNotPresentCount.get());
    }

    @Test
    public void whenCustomResolverDefined_quorumEventsFired() {
        EventCountingQuorumListener listener = new EventCountingQuorumListener(0, 1);
        QuorumListenerConfig listenerConfig = new QuorumListenerConfig(listener);
        String quorumName = randomString();

        QuorumFunction function = new QuorumFunction() {
            @Override
            public boolean apply(Collection<Member> members) {
                return members.size() >= 2;
            }
        };

        QuorumConfig quorumConfig = new QuorumConfig()
                .setName(quorumName)
                .setEnabled(true)
                .addListenerConfig(listenerConfig)
                .setQuorumFunctionImplementation(function);

        Config config = new Config().addQuorumConfig(quorumConfig);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config).shutdown();

        assertOpenEventually(listener.quorumNotPresent);
    }

    @Test
    public void quorumEventShowsCorrectMemberList() {
        final CountDownLatch quorumNotPresent = new CountDownLatch(1);
        final AtomicReference<Collection<Member>> eventMembersRef = new AtomicReference<Collection<Member>>();

        QuorumListener listener = new QuorumListener() {
            public void onChange(QuorumEvent quorumEvent) {
                if (!quorumEvent.isPresent()) {
                    assertEquals(3, quorumEvent.getThreshold());
                    Collection<Member> currentMembers = quorumEvent.getCurrentMembers();
                    eventMembersRef.set(currentMembers);
                    quorumNotPresent.countDown();
                }
            }
        };

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);

        HazelcastInstance hz1 = factory.newHazelcastInstance(configWithQuorum(randomName(), listener));
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        factory.newHazelcastInstance().shutdown();

        assertOpenEventually(quorumNotPresent);
        assertNotNull(eventMembersRef.get());

        Collection<Member> members = eventMembersRef.get();
        assertThat(members, Matchers.contains(hz1.getCluster().getLocalMember(), hz2.getCluster().getLocalMember()));
    }

    private Config configWithQuorum(String quorumName, QuorumListener listener) {
        QuorumListenerConfig listenerConfig = new QuorumListenerConfig();
        listenerConfig.setImplementation(listener);
        Config config = new Config();
        config.addQuorumConfig(new QuorumConfig(quorumName, true, 3).addListenerConfig(listenerConfig));
        return config;
    }

    private static class EventCountingQuorumListener implements QuorumListener {
        private final AtomicInteger quorumPresentCount = new AtomicInteger();
        private final AtomicInteger quorumNotPresentCount = new AtomicInteger();
        private final CountDownLatch quorumPresent;
        private final CountDownLatch quorumNotPresent;

        public EventCountingQuorumListener(int presentLatchCount, int notPresentLatchCount) {
            this.quorumPresent = new CountDownLatch(presentLatchCount);
            this.quorumNotPresent = new CountDownLatch(notPresentLatchCount);
        }

        public void onChange(QuorumEvent quorumEvent) {
            if (quorumEvent.isPresent()) {
                quorumPresentCount.incrementAndGet();
                quorumPresent.countDown();
            } else {
                quorumNotPresentCount.incrementAndGet();
                quorumNotPresent.countDown();
            }
        }
    }
}
