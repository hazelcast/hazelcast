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

package com.hazelcast.splitbrainprotection;

import com.hazelcast.config.Config;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.config.SplitBrainProtectionListenerConfig;
import com.hazelcast.cluster.Member;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractSplitBrainProtectionListenerTest extends HazelcastTestSupport {

    @Test
    public void testSplitBrainProtectionFailureEventFiredWhenNodeCountDropsBelowThreshold() {
        CountDownLatch splitBrainProtectionNotPresent = new CountDownLatch(1);
        String distributedObjectName = randomString();
        Config config = addSplitBrainProtection(new Config(), distributedObjectName, splitBrainProtectionListener(null, splitBrainProtectionNotPresent));
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance();
        factory.newHazelcastInstance().shutdown();
        assertOpenEventually(splitBrainProtectionNotPresent, 15);
    }

    @Test
    public void testSplitBrainProtectionEventsFiredWhenNodeCountBelowThenAboveThreshold() {
        CountDownLatch splitBrainProtectionNotPresent = new CountDownLatch(1);
        CountDownLatch splitBrainProtectionPresent = new CountDownLatch(1);
        String distributedObjectName = randomString();
        Config config = addSplitBrainProtection(new Config(), distributedObjectName, splitBrainProtectionListener(splitBrainProtectionPresent, splitBrainProtectionNotPresent));
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        assertOpenEventually(splitBrainProtectionNotPresent, 15);

        factory.newHazelcastInstance(config);
        assertOpenEventually(splitBrainProtectionPresent);
    }

    @Test
    public void testDifferentSplitBrainProtectionsGetCorrectEvents() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        CountDownLatch splitBrainProtectionFailureLatch = new CountDownLatch(2);
        Config config = new Config();
        addSplitBrainProtection(config, "fourNode", splitBrainProtectionListener(null, splitBrainProtectionFailureLatch));
        addSplitBrainProtection(config, "threeNode", splitBrainProtectionListener(null, splitBrainProtectionFailureLatch));
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        assertOpenEventually(splitBrainProtectionFailureLatch);
    }

    @Test
    public void testCustomResolverFiresSplitBrainProtectionFailureEvent() {
        CountDownLatch splitBrainProtectionNotPresent = new CountDownLatch(1);

        SplitBrainProtectionListenerConfig listenerConfig = new SplitBrainProtectionListenerConfig(splitBrainProtectionListener(null, splitBrainProtectionNotPresent));
        String distributedObjectName = randomString();
        String splitBrainProtectionName = randomString();
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig()
                .setName(splitBrainProtectionName)
                .setEnabled(true)
                .addListenerConfig(listenerConfig)
                .setFunctionImplementation(new SplitBrainProtectionFunction() {
                    @Override
                    public boolean apply(Collection<Member> members) {
                        return false;
                    }
                });
        Config config = new Config().addSplitBrainProtectionConfig(splitBrainProtectionConfig);
        addSplitBrainProtectionConfig(config, distributedObjectName, splitBrainProtectionName);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance();
        assertOpenEventually(splitBrainProtectionNotPresent, 15);
    }

    @Test
    public void testSplitBrainProtectionEventProvidesCorrectMemberListSize() {
        final CountDownLatch splitBrainProtectionNotPresent = new CountDownLatch(1);
        SplitBrainProtectionListenerConfig listenerConfig = new SplitBrainProtectionListenerConfig(new SplitBrainProtectionListener() {
            public void onChange(SplitBrainProtectionEvent splitBrainProtectionEvent) {
                if (!splitBrainProtectionEvent.isPresent()) {
                    Collection<Member> currentMembers = splitBrainProtectionEvent.getCurrentMembers();
                    assertEquals(3, splitBrainProtectionEvent.getThreshold());
                    assertTrue(currentMembers.size() < splitBrainProtectionEvent.getThreshold());
                    splitBrainProtectionNotPresent.countDown();
                }
            }
        });
        String distributedObjectName = randomString();
        String splitBrainProtectionName = randomString();
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig(splitBrainProtectionName, true, 3)
                .addListenerConfig(listenerConfig);
        Config config = new Config().addSplitBrainProtectionConfig(splitBrainProtectionConfig);
        addSplitBrainProtectionConfig(config, distributedObjectName, splitBrainProtectionName);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        assertOpenEventually(splitBrainProtectionNotPresent);
    }

    protected Config addSplitBrainProtection(Config config, String distributedObjectName, SplitBrainProtectionListener listener) {
        String splitBrainProtectionName = randomString();
        SplitBrainProtectionListenerConfig listenerConfig = new SplitBrainProtectionListenerConfig();
        listenerConfig.setImplementation(listener);
        config.addSplitBrainProtectionConfig(new SplitBrainProtectionConfig(splitBrainProtectionName, true, 3).addListenerConfig(listenerConfig));
        addSplitBrainProtectionConfig(config, distributedObjectName, splitBrainProtectionName);
        return config;
    }

    protected SplitBrainProtectionListener splitBrainProtectionListener(final CountDownLatch splitBrainProtectionPresent,
                                                                        final CountDownLatch splitBrainProtectionNotPresent) {
        return new SplitBrainProtectionListener() {
            public void onChange(SplitBrainProtectionEvent splitBrainProtectionEvent) {
                if (splitBrainProtectionEvent.isPresent()) {
                    if (splitBrainProtectionPresent != null) {
                        splitBrainProtectionPresent.countDown();
                    }
                } else {
                    if (splitBrainProtectionNotPresent != null) {
                        splitBrainProtectionNotPresent.countDown();
                    }
                }
            }
        };
    }

    protected abstract void addSplitBrainProtectionConfig(Config config, String distributedObjectName, String splitBrainProtectionName);
}
