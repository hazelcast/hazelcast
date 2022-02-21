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

package com.hazelcast.internal.eviction;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.eviction.CacheClearExpiredRecordsTask;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheConfiguration;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.instance.impl.LifecycleServiceImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListenerException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGING;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTTING_DOWN;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.lang.String.format;
import static javax.cache.expiry.Duration.ONE_HOUR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheExpirationManagerTest extends AbstractExpirationManagerTest {

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void restarts_running_backgroundClearTask_when_lifecycleState_turns_to_MERGED() {
        Config config = getConfig();
        config.setProperty(taskPeriodSecondsPropName(), "1");
        HazelcastInstance node = createHazelcastInstance(config);

        final SimpleEntryListener<Integer, Integer> simpleEntryListener = new SimpleEntryListener<Integer, Integer>();

        CacheManager cacheManager = createCacheManager(node);
        CacheConfiguration<Integer, Integer> cacheConfig = createCacheConfig(simpleEntryListener,
                new HazelcastExpiryPolicy(3000, 3000, 3000));
        Cache<Integer, Integer> cache = cacheManager.createCache("test", cacheConfig);

        cache.put(1, 1);

        ((LifecycleServiceImpl) node.getLifecycleService()).fireLifecycleEvent(MERGING);
        ((LifecycleServiceImpl) node.getLifecycleService()).fireLifecycleEvent(MERGED);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int expirationCount = simpleEntryListener.expiredCount.get();
                assertEquals(format("Expecting 1 expiration but found:%d", expirationCount), 1, expirationCount);
            }
        });
    }

    @Test
    public void clearExpiredRecordsTask_should_not_be_started_if_cache_has_no_expirable_records() {
        Config config = getConfig();
        config.setProperty(taskPeriodSecondsPropName(), "1");
        final HazelcastInstance node = createHazelcastInstance(config);

        CacheManager cacheManager = createCacheManager(node);
        cacheManager.createCache("test", new CacheConfig());

        assertTrueAllTheTime(() -> assertFalse("There should be zero CacheClearExpiredRecordsTask",
                hasClearExpiredRecordsTaskStarted(node)), 3);
    }

    @Test
    public void clearExpiredRecordsTask_should_not_be_started_when_disabled() {
        Config config = getConfig();
        config.setProperty(cleanupTaskEnabledPropName(), "false");
        final HazelcastInstance node = createHazelcastInstance(config);

        CacheManager cacheManager = createCacheManager(node);
        CacheConfiguration<Integer, Integer> cacheConfig = createCacheConfig(new SimpleEntryListener(),
                new HazelcastExpiryPolicy(3000, 3000, 3000));
        Cache<Integer, Integer> cache = cacheManager.createCache("test", cacheConfig);

        cache.put(1, 1);

        assertTrueAllTheTime(() -> assertFalse("There should be zero CacheClearExpiredRecordsTask",
                hasClearExpiredRecordsTaskStarted(node)), 3);
    }

    @Test
    public void clearExpiredRecordsTask_should_not_be_started_if_member_is_lite() {
        Config liteMemberConfig = getConfig();
        liteMemberConfig.setLiteMember(true);
        liteMemberConfig.setProperty(taskPeriodSecondsPropName(), "1");

        Config dataMemberConfig = getConfig();
        dataMemberConfig.setProperty(taskPeriodSecondsPropName(), "1");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final HazelcastInstance liteMember = factory.newHazelcastInstance(liteMemberConfig);
        factory.newHazelcastInstance(dataMemberConfig);

        CacheManager cacheManager = createCacheManager(liteMember);
        CacheConfiguration<Integer, Integer> cacheConfig = createCacheConfig(new SimpleEntryListener(),
                new HazelcastExpiryPolicy(1, 1, 1));
        Cache<Integer, Integer> cache = cacheManager.createCache("test", cacheConfig);
        cache.put(1, 1);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertFalse("There should be zero CacheClearExpiredRecordsTask",
                        hasClearExpiredRecordsTaskStarted(liteMember));
            }
        }, 3);
    }

    @Test
    public void stops_running_backgroundClearTask_when_lifecycleState_SHUTTING_DOWN() {
        backgroundClearTaskStops_whenLifecycleState(SHUTTING_DOWN);
    }

    @Test
    public void stops_running_backgroundClearTask_when_lifecycleState_MERGING() {
        backgroundClearTaskStops_whenLifecycleState(MERGING);
    }

    @Test
    public void no_expiration_task_starts_on_new_node_after_migration_when_there_is_no_expirable_entry() {
        Config config = getConfig();
        config.setProperty(taskPeriodSecondsPropName(), "1");
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final HazelcastInstance node1 = factory.newHazelcastInstance(config);

        CacheManager cacheManager = createCacheManager(node1);
        Cache cache = cacheManager.createCache("test", new CacheConfig());
        cache.put(1, 1);

        final HazelcastInstance node2 = factory.newHazelcastInstance(config);
        node1.shutdown();

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertFalse("There should be zero CacheClearExpiredRecordsTask",
                        hasClearExpiredRecordsTaskStarted(node2));
            }
        }, 3);
    }

    @Test
    public void expiration_task_starts_on_new_node_after_migration_when_there_is_expirable_entry() {
        Config config = getConfig();
        config.setProperty(taskPeriodSecondsPropName(), "1");
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final HazelcastInstance node1 = factory.newHazelcastInstance(config);

        CacheManager cacheManager = createCacheManager(node1);
        Cache cache = cacheManager.createCache("test", new CacheConfig());
        ((ICache) cache).put(1, 1, new HazelcastExpiryPolicy(ONE_HOUR, ONE_HOUR, ONE_HOUR));

        final HazelcastInstance node2 = factory.newHazelcastInstance(config);
        node1.shutdown();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue("There should be one ClearExpiredRecordsTask started",
                        hasClearExpiredRecordsTaskStarted(node2));
            }
        });
    }

    private void backgroundClearTaskStops_whenLifecycleState(LifecycleEvent.LifecycleState lifecycleState) {
        Config config = getConfig();
        config.setProperty(taskPeriodSecondsPropName(), "1");
        HazelcastInstance node = createHazelcastInstance(config);

        final SimpleEntryListener simpleEntryListener = new SimpleEntryListener();

        CacheManager cacheManager = createCacheManager(node);
        CacheConfiguration cacheConfiguration = createCacheConfig(simpleEntryListener, new HazelcastExpiryPolicy(1000, 1000, 1000));
        Cache<Integer, Integer> cache = cacheManager.createCache("test", cacheConfiguration);
        cache.put(1, 1);

        ((LifecycleServiceImpl) node.getLifecycleService()).fireLifecycleEvent(lifecycleState);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                int expirationCount = simpleEntryListener.expiredCount.get();
                assertEquals(format("Expecting no expiration but found:%d", expirationCount), 0, expirationCount);
            }
        }, 5);
    }

    private boolean hasClearExpiredRecordsTaskStarted(HazelcastInstance node) {
        CacheService service = getNodeEngineImpl(node).getService(CacheService.SERVICE_NAME);
        return service.getExpirationManager().isScheduled();
    }

    protected CacheManager createCacheManager(HazelcastInstance instance) {
        return createServerCachingProvider(instance).getCacheManager();
    }

    @Override
    protected String cleanupOperationCountPropName() {
        return CacheClearExpiredRecordsTask.PROP_CLEANUP_OPERATION_COUNT;
    }

    @Override
    protected String taskPeriodSecondsPropName() {
        return CacheClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
    }

    @Override
    protected String cleanupPercentagePropName() {
        return CacheClearExpiredRecordsTask.PROP_CLEANUP_PERCENTAGE;
    }

    @Override
    protected String cleanupTaskEnabledPropName() {
        return CacheClearExpiredRecordsTask.PROP_CLEANUP_ENABLED;
    }

    @Override
    protected ExpirationManager newExpirationManager(HazelcastInstance node) {
        return new ExpirationManager(new CacheClearExpiredRecordsTask(getPartitionSegments(node), getNodeEngineImpl(node)),
                getNodeEngineImpl(node));
    }

    @Override
    protected AtomicInteger configureForTurnsActivePassiveTest(HazelcastInstance node) {
        final SimpleEntryListener simpleEntryListener = new SimpleEntryListener();

        CacheManager cacheManager = createCacheManager(node);
        CacheConfiguration cacheConfiguration = createCacheConfig(simpleEntryListener, new HazelcastExpiryPolicy(3000, 3000, 3000));
        Cache<Integer, Integer> cache = cacheManager.createCache("test", cacheConfiguration);
        cache.put(1, 1);

        return simpleEntryListener.expiredCount;
    }

    protected CachePartitionSegment[] getPartitionSegments(HazelcastInstance instance) {
        return ((CacheService) getNodeEngineImpl(instance)
                                        .getService(CacheService.SERVICE_NAME))
                .getPartitionSegments();
    }

    protected CacheConfiguration createCacheConfig(SimpleEntryListener simpleEntryListener, HazelcastExpiryPolicy expiryPolicy) {
        return new CacheConfig<Integer, Integer>()
                .addCacheEntryListenerConfiguration(
                        new MutableCacheEntryListenerConfiguration(
                                FactoryBuilder.factoryOf(simpleEntryListener),
                                null,
                                false,
                                false
                        )
                ).setExpiryPolicyFactory(FactoryBuilder.factoryOf(expiryPolicy));
    }

    public static class SimpleEntryListener<K, V> implements CacheEntryExpiredListener<K, V>, Serializable {

        AtomicInteger expiredCount = new AtomicInteger();

        @Override
        public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws CacheEntryListenerException {
            expiredCount.incrementAndGet();
        }
    }
}
