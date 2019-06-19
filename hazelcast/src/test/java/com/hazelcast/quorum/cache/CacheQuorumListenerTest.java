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

package com.hazelcast.quorum.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.QuorumListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.Member;
import com.hazelcast.quorum.QuorumEvent;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumFunction;
import com.hazelcast.quorum.QuorumListener;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheQuorumListenerTest extends HazelcastTestSupport {

    @Test
    public void testQuorumFailureEventFiredWhenNodeCountBelowThreshold() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Config config = new Config();
        QuorumListenerConfig listenerConfig = new QuorumListenerConfig();
        listenerConfig.setImplementation(new QuorumListener() {
            public void onChange(QuorumEvent quorumEvent) {
                if (!quorumEvent.isPresent()) {
                    countDownLatch.countDown();
                }
            }
        });
        String cacheName = randomString();
        String quorumName = randomString();
        QuorumConfig quorumConfig = new QuorumConfig(quorumName, true, 3);
        quorumConfig.addListenerConfig(listenerConfig);
        config.getCacheConfig(cacheName).setQuorumName(quorumName);
        config.addQuorumConfig(quorumConfig);
        HazelcastInstance instance = createHazelcastInstance(config);

        HazelcastServerCachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(instance);
        Cache<Object, Object> cache = cachingProvider.getCacheManager().getCache(cacheName);
        try {
            cache.put(generateKeyOwnedBy(instance), 1);
            fail("Expected a QuorumException");
        } catch (QuorumException expected) {
            ignore(expected);
        }

        assertOpenEventually(countDownLatch, 15);
    }

    @Test
    public void testQuorumEventsFiredWhenNodeCountBelowThenAboveThreshold() {
        final CountDownLatch belowLatch = new CountDownLatch(1);
        final CountDownLatch aboveLatch = new CountDownLatch(1);
        Config config = new Config();
        QuorumListenerConfig listenerConfig = new QuorumListenerConfig();
        listenerConfig.setImplementation(new QuorumListener() {
            public void onChange(QuorumEvent quorumEvent) {
                if (quorumEvent.isPresent()) {
                    aboveLatch.countDown();
                } else {
                    belowLatch.countDown();
                }
            }
        });
        String cacheName = randomString();
        String quorumName = randomString();
        QuorumConfig quorumConfig = new QuorumConfig(quorumName, true, 3);
        quorumConfig.addListenerConfig(listenerConfig);
        config.getCacheConfig(cacheName).setQuorumName(quorumName);
        config.addQuorumConfig(quorumConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastServerCachingProvider cachingProvider1 = HazelcastServerCachingProvider.createCachingProvider(instance1);
        Cache<Object, Object> cache = cachingProvider1.getCacheManager().getCache(cacheName);
        try {
            cache.put(generateKeyOwnedBy(instance1), 1);
            fail("Expected a QuorumException");
        } catch (QuorumException expected) {
            ignore(expected);
        }
        assertOpenEventually(belowLatch, 15);

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        cache.put(generateKeyOwnedBy(instance1), 1);
        assertOpenEventually(aboveLatch, 15);
    }

    @Test
    public void testDifferentQuorumsGetCorrectEvents() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final CountDownLatch quorumFailureLatch = new CountDownLatch(2);
        String fourNodeQuorumName = "fourNode";
        QuorumConfig fourNodeQuorumConfig = new QuorumConfig(fourNodeQuorumName, true, 4);
        fourNodeQuorumConfig.addListenerConfig(new QuorumListenerConfig(new QuorumListener() {
            public void onChange(QuorumEvent quorumEvent) {
                if (!quorumEvent.isPresent()) {
                    quorumFailureLatch.countDown();
                }
            }
        }));
        String threeNodeQuorumName = "threeNode";
        QuorumConfig threeNodeQuorumConfig = new QuorumConfig(threeNodeQuorumName, true, 3);
        threeNodeQuorumConfig.addListenerConfig(new QuorumListenerConfig(new QuorumListener() {
            public void onChange(QuorumEvent quorumEvent) {
                if (!quorumEvent.isPresent()) {
                    quorumFailureLatch.countDown();
                }
            }
        }));
        CacheSimpleConfig fourNodeCacheConfig = new CacheSimpleConfig();
        fourNodeCacheConfig.setName("fourNode");
        fourNodeCacheConfig.setQuorumName(fourNodeQuorumName);

        CacheSimpleConfig threeNodeCacheConfig = new CacheSimpleConfig();
        threeNodeCacheConfig.setName("threeNode");
        threeNodeCacheConfig.setQuorumName(threeNodeQuorumName);

        Config config = new Config();
        config.addCacheConfig(fourNodeCacheConfig);
        config.addQuorumConfig(fourNodeQuorumConfig);
        config.addCacheConfig(threeNodeCacheConfig);
        config.addQuorumConfig(threeNodeQuorumConfig);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        HazelcastServerCachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(h1);
        Cache<Object, Object> fourNode = cachingProvider.getCacheManager().getCache("fourNode");
        Cache<Object, Object> threeNode = cachingProvider.getCacheManager().getCache("threeNode");
        try {
            threeNode.put(generateKeyOwnedBy(h1), "bar");
            fail("Expected a QuorumException");
        } catch (QuorumException expected) {
            ignore(expected);
        }
        try {
            fourNode.put(generateKeyOwnedBy(h1), "bar");
            fail("Expected a QuorumException");
        } catch (QuorumException expected) {
            ignore(expected);
        }
        assertOpenEventually(quorumFailureLatch, 15);
    }

    @Test
    public void testCustomResolverFiresQuorumFailureEvent() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Config config = new Config();
        QuorumListenerConfig listenerConfig = new QuorumListenerConfig();
        listenerConfig.setImplementation(new QuorumListener() {
            @Override
            public void onChange(QuorumEvent quorumEvent) {
                if (!quorumEvent.isPresent()) {
                    countDownLatch.countDown();
                }
            }
        });
        String cacheName = randomString();
        String quorumName = randomString();
        QuorumConfig quorumConfig = new QuorumConfig();
        quorumConfig.setName(quorumName);
        quorumConfig.setEnabled(true);
        quorumConfig.addListenerConfig(listenerConfig);
        quorumConfig.setQuorumFunctionImplementation(new QuorumFunction() {
            @Override
            public boolean apply(Collection<Member> members) {
                return false;
            }
        });
        config.getCacheConfig(cacheName).setQuorumName(quorumName);
        config.addQuorumConfig(quorumConfig);

        HazelcastInstance instance = createHazelcastInstance(config);
        HazelcastServerCachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(instance);
        Cache<Object, Object> cache = cachingProvider.getCacheManager().getCache(cacheName);
        try {
            cache.put(generateKeyOwnedBy(instance), 1);
            fail("Expected a QuorumException");
        } catch (QuorumException expected) {
            ignore(expected);
        }

        assertOpenEventually(countDownLatch, 15);
    }

    @Test
    public void testQuorumEventProvidesCorrectMemberListSize() {
        final CountDownLatch belowLatch = new CountDownLatch(1);
        Config config = new Config();
        QuorumListenerConfig listenerConfig = new QuorumListenerConfig();
        listenerConfig.setImplementation(new QuorumListener() {
            public void onChange(QuorumEvent quorumEvent) {
                if (!quorumEvent.isPresent()) {
                    Collection<Member> currentMembers = quorumEvent.getCurrentMembers();
                    assertEquals(3, quorumEvent.getThreshold());
                    assertTrue(currentMembers.size() < quorumEvent.getThreshold());
                    belowLatch.countDown();
                }
            }
        });
        String cacheName = randomString();
        String quorumName = randomString();
        QuorumConfig quorumConfig = new QuorumConfig(quorumName, true, 3);
        quorumConfig.addListenerConfig(listenerConfig);
        config.getCacheConfig(cacheName).setQuorumName(quorumName);
        config.addQuorumConfig(quorumConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        HazelcastServerCachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(instance1);
        Cache<Object, Object> cache = cachingProvider.getCacheManager().getCache(cacheName);
        try {
            cache.put(generateKeyOwnedBy(instance1), 1);
            fail("Expected a QuorumException");
        } catch (QuorumException expected) {
            ignore(expected);
        }

        assertOpenEventually(belowLatch, 15);
    }
}
