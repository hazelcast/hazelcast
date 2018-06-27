/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.eviction;

import com.hazelcast.cache.CacheTestSupport;
import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.backup.BackupAccessor;
import com.hazelcast.test.backup.TestBackupUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cache.impl.eviction.CacheClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.backup.TestBackupUtils.assertBackupSizeEventually;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheExpirationTest extends CacheTestSupport {

    private final Duration FIVE_SECONDS = new Duration(TimeUnit.SECONDS, 5);

    @Rule
    public final OverridePropertyRule overrideTaskSecondsRule = set(PROP_TASK_PERIOD_SECONDS, "1");

    private static final int NINSTANCE = 3;
    private static final int KEY_RANGE = 10000;

    private HazelcastInstance[] instances = new HazelcastInstance[3];
    private TestHazelcastInstanceFactory factory;

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return instances[0];
    }

    @Override
    protected void onSetup() {
        factory = createHazelcastInstanceFactory(3);
        for (int i = 0; i < NINSTANCE; i++) {
            instances[i] = factory.newHazelcastInstance(getConfig());
        }
    }

    @Override
    protected void onTearDown() {
        factory.shutdownAll();
    }

    protected <K, V, M extends Serializable & ExpiryPolicy, T extends Serializable & CacheEntryListener<K, V>>
    CacheConfig<K, V> createCacheConfig(M expiryPolicy, T listener) {
        CacheConfig<K, V> cacheConfig = new CacheConfig<K, V>();
        cacheConfig.setExpiryPolicyFactory(FactoryBuilder.factoryOf(expiryPolicy));
        cacheConfig.setName(randomName());
        if (listener != null) {
            MutableCacheEntryListenerConfiguration<K, V> listenerConfiguration = new MutableCacheEntryListenerConfiguration<K, V>(
                    FactoryBuilder.factoryOf(listener), null, true, true
            );
            cacheConfig.addCacheEntryListenerConfiguration(listenerConfiguration);
        }
        cacheConfig.setBackupCount(NINSTANCE - 1);
        return cacheConfig;
    }

    @Test
    public void testSimpleExpiration() {
        SimpleExpiryListener listener = new SimpleExpiryListener();
        CacheConfig cacheConfig = createCacheConfig(new HazelcastExpiryPolicy(1, 1, 1), listener);
        Cache cache = createCache(cacheConfig);
        cache.put("key", "value");

        assertEqualsEventually(1, listener.getExpirationCount());
    }

    @Test
    public void testBackupsAreEmptyAfterExpiration() {
        SimpleExpiryListener listener = new SimpleExpiryListener();
        CacheConfig cacheConfig = createCacheConfig(new HazelcastExpiryPolicy(100, 100, 100), listener);
        Cache cache = createCache(cacheConfig);

        for (int i = 0; i < KEY_RANGE; i++) {
            cache.put(i, i);
        }

        assertEqualsEventually(KEY_RANGE, listener.getExpirationCount());
        for (int i = 1; i < NINSTANCE; i++) {
            BackupAccessor backupAccessor = TestBackupUtils.newCacheAccessor(instances, cache.getName(), i);
            assertBackupSizeEventually(0, backupAccessor);
        }
    }

    @Test
    public void test_whenEntryIsAccessedBackupIsNotCleaned() {
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(new HazelcastExpiryPolicy(FIVE_SECONDS, Duration.ETERNAL, FIVE_SECONDS), null);
        Cache<Integer, Integer> cache = createCache(cacheConfig);

        for (int i = 0; i < KEY_RANGE; i++) {
            cache.put(i, i);
            cache.get(i);
        }

        sleepAtLeastSeconds(5);

        for (int i = 1; i < NINSTANCE; i++) {
            BackupAccessor backupAccessor = TestBackupUtils.newCacheAccessor(instances, cache.getName(), i);
            for (int j = 0; j < KEY_RANGE; j++) {
                assertEquals(i, backupAccessor.get(i));
            }
        }
    }

    @Test
    public void test_whenEntryIsUpdatedBackupIsNotCleaned() {
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(new HazelcastExpiryPolicy(FIVE_SECONDS, FIVE_SECONDS, Duration.ETERNAL), null);
        Cache<Integer, Integer> cache = createCache(cacheConfig);

        for (int i = 0; i < KEY_RANGE; i++) {
            cache.put(i, i);
            cache.put(i, i);
        }

        cache.put(1, 1);
        sleepAtLeastSeconds(5);

        for (int i = 1; i < NINSTANCE; i++) {
            BackupAccessor backupAccessor = TestBackupUtils.newCacheAccessor(instances, cache.getName(), i);
            for (int j = 0; j < KEY_RANGE; j++) {
                assertEquals(i, backupAccessor.get(i));
            }
        }
    }

    public static class SimpleExpiryListener<K, V> implements CacheEntryExpiredListener<K, V>, Serializable {

        private AtomicInteger expirationCount = new AtomicInteger();

        @Override
        public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws CacheEntryListenerException {
            expirationCount.incrementAndGet();
        }

        public AtomicInteger getExpirationCount() {
            return expirationCount;
        }
    }
}
