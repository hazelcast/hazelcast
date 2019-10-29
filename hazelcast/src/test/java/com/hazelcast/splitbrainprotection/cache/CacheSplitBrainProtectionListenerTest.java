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

package com.hazelcast.splitbrainprotection.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.config.SplitBrainProtectionListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.Member;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionEvent;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionFunction;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionListener;
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

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheSplitBrainProtectionListenerTest extends HazelcastTestSupport {

    @Test
    public void testSplitBrainProtectionFailureEventFiredWhenNodeCountBelowThreshold() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Config config = new Config();
        SplitBrainProtectionListenerConfig listenerConfig = new SplitBrainProtectionListenerConfig();
        listenerConfig.setImplementation(new SplitBrainProtectionListener() {
            public void onChange(SplitBrainProtectionEvent splitBrainProtectionEvent) {
                if (!splitBrainProtectionEvent.isPresent()) {
                    countDownLatch.countDown();
                }
            }
        });
        String cacheName = randomString();
        String splitBrainProtectionName = randomString();
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig(splitBrainProtectionName, true, 3);
        splitBrainProtectionConfig.addListenerConfig(listenerConfig);
        config.getCacheConfig(cacheName).setSplitBrainProtectionName(splitBrainProtectionName);
        config.addSplitBrainProtectionConfig(splitBrainProtectionConfig);
        HazelcastInstance instance = createHazelcastInstance(config);

        HazelcastServerCachingProvider cachingProvider = createServerCachingProvider(instance);
        Cache<Object, Object> cache = cachingProvider.getCacheManager().getCache(cacheName);
        try {
            cache.put(generateKeyOwnedBy(instance), 1);
            fail("Expected a SplitBrainProtectionException");
        } catch (SplitBrainProtectionException expected) {
            ignore(expected);
        }

        assertOpenEventually(countDownLatch, 15);
    }

    @Test
    public void testSplitBrainProtectionEventsFiredWhenNodeCountBelowThenAboveThreshold() {
        final CountDownLatch belowLatch = new CountDownLatch(1);
        final CountDownLatch aboveLatch = new CountDownLatch(1);
        Config config = new Config();
        SplitBrainProtectionListenerConfig listenerConfig = new SplitBrainProtectionListenerConfig();
        listenerConfig.setImplementation(new SplitBrainProtectionListener() {
            public void onChange(SplitBrainProtectionEvent splitBrainProtectionEvent) {
                if (splitBrainProtectionEvent.isPresent()) {
                    aboveLatch.countDown();
                } else {
                    belowLatch.countDown();
                }
            }
        });
        String cacheName = randomString();
        String splitBrainProtectionName = randomString();
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig(splitBrainProtectionName, true, 3);
        splitBrainProtectionConfig.addListenerConfig(listenerConfig);
        config.getCacheConfig(cacheName).setSplitBrainProtectionName(splitBrainProtectionName);
        config.addSplitBrainProtectionConfig(splitBrainProtectionConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastServerCachingProvider cachingProvider1 = createServerCachingProvider(instance1);
        Cache<Object, Object> cache = cachingProvider1.getCacheManager().getCache(cacheName);
        try {
            cache.put(generateKeyOwnedBy(instance1), 1);
            fail("Expected a SplitBrainProtectionException");
        } catch (SplitBrainProtectionException expected) {
            ignore(expected);
        }
        assertOpenEventually(belowLatch, 15);

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        cache.put(generateKeyOwnedBy(instance1), 1);
        assertOpenEventually(aboveLatch, 15);
    }

    @Test
    public void testDifferentSplitBrainProtectionsGetCorrectEvents() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final CountDownLatch splitBrainProtectionFailureLatch = new CountDownLatch(2);
        String fourNodeSplitBrainProtectionName = "fourNode";
        SplitBrainProtectionConfig fourNodeSplitBrainProtectionConfig = new SplitBrainProtectionConfig(fourNodeSplitBrainProtectionName, true, 4);
        fourNodeSplitBrainProtectionConfig.addListenerConfig(new SplitBrainProtectionListenerConfig(new SplitBrainProtectionListener() {
            public void onChange(SplitBrainProtectionEvent splitBrainProtectionEvent) {
                if (!splitBrainProtectionEvent.isPresent()) {
                    splitBrainProtectionFailureLatch.countDown();
                }
            }
        }));
        String threeNodeSplitBrainProtectionName = "threeNode";
        SplitBrainProtectionConfig threeNodeSplitBrainProtectionConfig = new SplitBrainProtectionConfig(threeNodeSplitBrainProtectionName, true, 3);
        threeNodeSplitBrainProtectionConfig.addListenerConfig(new SplitBrainProtectionListenerConfig(new SplitBrainProtectionListener() {
            public void onChange(SplitBrainProtectionEvent splitBrainProtectionEvent) {
                if (!splitBrainProtectionEvent.isPresent()) {
                    splitBrainProtectionFailureLatch.countDown();
                }
            }
        }));
        CacheSimpleConfig fourNodeCacheConfig = new CacheSimpleConfig();
        fourNodeCacheConfig.setName("fourNode");
        fourNodeCacheConfig.setSplitBrainProtectionName(fourNodeSplitBrainProtectionName);

        CacheSimpleConfig threeNodeCacheConfig = new CacheSimpleConfig();
        threeNodeCacheConfig.setName("threeNode");
        threeNodeCacheConfig.setSplitBrainProtectionName(threeNodeSplitBrainProtectionName);

        Config config = new Config();
        config.addCacheConfig(fourNodeCacheConfig);
        config.addSplitBrainProtectionConfig(fourNodeSplitBrainProtectionConfig);
        config.addCacheConfig(threeNodeCacheConfig);
        config.addSplitBrainProtectionConfig(threeNodeSplitBrainProtectionConfig);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        HazelcastServerCachingProvider cachingProvider = createServerCachingProvider(h1);
        Cache<Object, Object> fourNode = cachingProvider.getCacheManager().getCache("fourNode");
        Cache<Object, Object> threeNode = cachingProvider.getCacheManager().getCache("threeNode");
        try {
            threeNode.put(generateKeyOwnedBy(h1), "bar");
            fail("Expected a SplitBrainProtectionException");
        } catch (SplitBrainProtectionException expected) {
            ignore(expected);
        }
        try {
            fourNode.put(generateKeyOwnedBy(h1), "bar");
            fail("Expected a SplitBrainProtectionException");
        } catch (SplitBrainProtectionException expected) {
            ignore(expected);
        }
        assertOpenEventually(splitBrainProtectionFailureLatch, 15);
    }

    @Test
    public void testCustomResolverFiresSplitBrainProtectionFailureEvent() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Config config = new Config();
        SplitBrainProtectionListenerConfig listenerConfig = new SplitBrainProtectionListenerConfig();
        listenerConfig.setImplementation(new SplitBrainProtectionListener() {
            @Override
            public void onChange(SplitBrainProtectionEvent splitBrainProtectionEvent) {
                if (!splitBrainProtectionEvent.isPresent()) {
                    countDownLatch.countDown();
                }
            }
        });
        String cacheName = randomString();
        String splitBrainProtectionName = randomString();
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig();
        splitBrainProtectionConfig.setName(splitBrainProtectionName);
        splitBrainProtectionConfig.setEnabled(true);
        splitBrainProtectionConfig.addListenerConfig(listenerConfig);
        splitBrainProtectionConfig.setFunctionImplementation(new SplitBrainProtectionFunction() {
            @Override
            public boolean apply(Collection<Member> members) {
                return false;
            }
        });
        config.getCacheConfig(cacheName).setSplitBrainProtectionName(splitBrainProtectionName);
        config.addSplitBrainProtectionConfig(splitBrainProtectionConfig);

        HazelcastInstance instance = createHazelcastInstance(config);
        HazelcastServerCachingProvider cachingProvider = createServerCachingProvider(instance);
        Cache<Object, Object> cache = cachingProvider.getCacheManager().getCache(cacheName);
        try {
            cache.put(generateKeyOwnedBy(instance), 1);
            fail("Expected a SplitBrainProtectionException");
        } catch (SplitBrainProtectionException expected) {
            ignore(expected);
        }

        assertOpenEventually(countDownLatch, 15);
    }

    @Test
    public void testSplitBrainProtectionEventProvidesCorrectMemberListSize() {
        final CountDownLatch belowLatch = new CountDownLatch(1);
        Config config = new Config();
        SplitBrainProtectionListenerConfig listenerConfig = new SplitBrainProtectionListenerConfig();
        listenerConfig.setImplementation(new SplitBrainProtectionListener() {
            public void onChange(SplitBrainProtectionEvent splitBrainProtectionEvent) {
                if (!splitBrainProtectionEvent.isPresent()) {
                    Collection<Member> currentMembers = splitBrainProtectionEvent.getCurrentMembers();
                    assertEquals(3, splitBrainProtectionEvent.getThreshold());
                    assertTrue(currentMembers.size() < splitBrainProtectionEvent.getThreshold());
                    belowLatch.countDown();
                }
            }
        });
        String cacheName = randomString();
        String splitBrainProtectionName = randomString();
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig(splitBrainProtectionName, true, 3);
        splitBrainProtectionConfig.addListenerConfig(listenerConfig);
        config.getCacheConfig(cacheName).setSplitBrainProtectionName(splitBrainProtectionName);
        config.addSplitBrainProtectionConfig(splitBrainProtectionConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        HazelcastServerCachingProvider cachingProvider = createServerCachingProvider(instance1);
        Cache<Object, Object> cache = cachingProvider.getCacheManager().getCache(cacheName);
        try {
            cache.put(generateKeyOwnedBy(instance1), 1);
            fail("Expected a SplitBrainProtectionException");
        } catch (SplitBrainProtectionException expected) {
            ignore(expected);
        }

        assertOpenEventually(belowLatch, 15);
    }
}
