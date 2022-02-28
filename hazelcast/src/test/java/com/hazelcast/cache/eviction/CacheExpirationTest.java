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

package com.hazelcast.cache.eviction;

import com.hazelcast.cache.CacheTestSupport;
import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.backup.BackupAccessor;
import com.hazelcast.test.backup.TestBackupUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.expiry.Duration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cache.impl.eviction.CacheClearExpiredRecordsTask.PROP_CLEANUP_OPERATION_COUNT;
import static com.hazelcast.cache.impl.eviction.CacheClearExpiredRecordsTask.PROP_CLEANUP_PERCENTAGE;
import static com.hazelcast.cache.impl.eviction.CacheClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.backup.TestBackupUtils.assertBackupSizeEventually;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheExpirationTest extends CacheTestSupport {

    @Rule
    public final OverridePropertyRule overrideTaskSecondsRule = set(PROP_TASK_PERIOD_SECONDS, "1");

    @Rule
    public final OverridePropertyRule overrideCleanupPercentage = set(PROP_CLEANUP_PERCENTAGE, "100");

    @Rule
    public final OverridePropertyRule overrideCleanupOperationCount = set(PROP_CLEANUP_OPERATION_COUNT, "1000");

    @Parameterized.Parameters(name = "useSyncBackups:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true},
                {false}
        });
    }

    @Parameterized.Parameter(0)
    public boolean useSyncBackups;

    protected static final int KEY_RANGE = 1000;
    protected static final int CLUSTER_SIZE = 2;
    protected final Duration THREE_SECONDS = new Duration(TimeUnit.SECONDS, 3);

    protected TestHazelcastInstanceFactory factory;
    protected HazelcastInstance[] instances = new HazelcastInstance[3];

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return instances[0];
    }

    @Override
    protected void onSetup() {
        factory = createHazelcastInstanceFactory(CLUSTER_SIZE);
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            instances[i] = factory.newHazelcastInstance(getConfig());
        }
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Override
    protected void onTearDown() {
        factory.shutdownAll();
    }

    protected <K, V, M extends Serializable & ExpiryPolicy, T extends Serializable & CacheEntryListener<K, V>>
    CacheConfig<K, V> createCacheConfig(M expiryPolicy, T listener) {
        CacheConfig<K, V> cacheConfig = createCacheConfig(expiryPolicy);
        MutableCacheEntryListenerConfiguration<K, V> listenerConfiguration = new MutableCacheEntryListenerConfiguration<K, V>(
                FactoryBuilder.factoryOf(listener), null, true, true
        );
        cacheConfig.addCacheEntryListenerConfiguration(listenerConfiguration);
        return cacheConfig;
    }

    protected <K, V, M extends Serializable & ExpiryPolicy> CacheConfig<K, V> createCacheConfig(M expiryPolicy) {
        CacheConfig<K, V> cacheConfig = new CacheConfig<>();
        cacheConfig.setExpiryPolicyFactory(FactoryBuilder.factoryOf(expiryPolicy));
        cacheConfig.setName(randomName());

        if (useSyncBackups) {
            cacheConfig.setBackupCount(CLUSTER_SIZE - 1);
            cacheConfig.setAsyncBackupCount(0);
        } else {
            cacheConfig.setBackupCount(0);
            cacheConfig.setAsyncBackupCount(CLUSTER_SIZE - 1);
        }

        return cacheConfig;
    }

    @Test
    public void testSimpleExpiration_put() {
        SimpleExpiryListener listener = new SimpleExpiryListener();
        CacheConfig<String, String> cacheConfig = createCacheConfig(new HazelcastExpiryPolicy(1, 1, 1), listener);
        Cache<String, String> cache = createCache(cacheConfig);
        cache.put("key", "value");

        assertEqualsEventually(1, listener.getExpirationCount());
    }

    @Test
    public void testSimpleExpiration_putAsync() {
        SimpleExpiryListener listener = new SimpleExpiryListener();
        CacheConfig<String, String> cacheConfig = createCacheConfig(new HazelcastExpiryPolicy(1, 1, 1), listener);
        Cache<String, String> cache = createCache(cacheConfig);
        ((ICache<String, String>) cache).putAsync("key", "value");

        assertEqualsEventually(1, listener.getExpirationCount());
    }

    @Test
    public void testSimpleExpiration_putAll() {
        SimpleExpiryListener listener = new SimpleExpiryListener();
        CacheConfig<String, String> cacheConfig = createCacheConfig(new HazelcastExpiryPolicy(1, 1, 1), listener);
        Cache<String, String> cache = createCache(cacheConfig);

        Map<String, String> entries = new HashMap<String, String>();
        entries.put("key1", "value1");
        entries.put("key2", "value2");
        cache.putAll(entries);

        assertEqualsEventually(2, listener.getExpirationCount());
    }

    @Test
    public void testSimpleExpiration_getAndPut() {
        SimpleExpiryListener listener = new SimpleExpiryListener();
        CacheConfig<String, String> cacheConfig = createCacheConfig(new HazelcastExpiryPolicy(1, 1, 1), listener);
        Cache<String, String> cache = createCache(cacheConfig);
        cache.getAndPut("key", "value");

        assertEqualsEventually(1, listener.getExpirationCount());
    }

    @Test
    public void testSimpleExpiration_getAndPutAsync() {
        SimpleExpiryListener listener = new SimpleExpiryListener();
        CacheConfig<String, String> cacheConfig = createCacheConfig(new HazelcastExpiryPolicy(1, 1, 1), listener);
        Cache<String, String> cache = createCache(cacheConfig);
        ((ICache<String, String>) cache).getAndPutAsync("key", "value");

        assertEqualsEventually(1, listener.getExpirationCount());
    }

    @Test
    public void testSimpleExpiration_getAndReplace() {
        SimpleExpiryListener listener = new SimpleExpiryListener();
        CacheConfig<String, String> cacheConfig = createCacheConfig(new EternalExpiryPolicy(), listener);
        Cache<String, String> cache = createCache(cacheConfig);
        cache.put("key", "value");
        cache.unwrap(ICache.class).getAndReplace("key", "value", new HazelcastExpiryPolicy(1, 1, 1));

        assertEqualsEventually(1, listener.getExpirationCount());
    }

    @Test
    public void testSimpleExpiration_getAndReplaceAsync() {
        SimpleExpiryListener listener = new SimpleExpiryListener();
        CacheConfig<String, String> cacheConfig = createCacheConfig(new EternalExpiryPolicy(), listener);
        Cache<String, String> cache = createCache(cacheConfig);
        cache.put("key", "value");
        cache.unwrap(ICache.class).getAndReplaceAsync("key", "value", new HazelcastExpiryPolicy(1, 1, 1));

        assertEqualsEventually(1, listener.getExpirationCount());
    }

    @Test
    public void testSimpleExpiration_putIfAbsent() {
        SimpleExpiryListener listener = new SimpleExpiryListener();
        CacheConfig<String, String> cacheConfig = createCacheConfig(new HazelcastExpiryPolicy(1, 1, 1), listener);
        Cache<String, String> cache = createCache(cacheConfig);
        cache.putIfAbsent("key", "value");

        assertEqualsEventually(1, listener.getExpirationCount());
    }

    @Test
    public void testSimpleExpiration_putIfAbsentAsync() {
        SimpleExpiryListener listener = new SimpleExpiryListener();
        CacheConfig<String, String> cacheConfig = createCacheConfig(new HazelcastExpiryPolicy(1, 1, 1), listener);
        Cache<String, String> cache = createCache(cacheConfig);
        ((ICache<String, String>) cache).putIfAbsentAsync("key", "value");

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
        for (int i = 1; i < CLUSTER_SIZE; i++) {
            BackupAccessor backupAccessor = TestBackupUtils.newCacheAccessor(instances, cache.getName(), i);
            assertBackupSizeEventually(0, backupAccessor);
        }
    }

    @Test
    public void test_whenEntryIsAccessedBackupIsNotCleaned() {
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(new HazelcastExpiryPolicy(THREE_SECONDS, Duration.ETERNAL, THREE_SECONDS));
        Cache<Integer, Integer> cache = createCache(cacheConfig);

        for (int i = 0; i < KEY_RANGE; i++) {
            cache.put(i, i);
            cache.get(i);
        }

        sleepAtLeastSeconds(3);

        for (int i = 1; i < CLUSTER_SIZE; i++) {
            BackupAccessor backupAccessor = TestBackupUtils.newCacheAccessor(instances, cache.getName(), i);
            for (int j = 0; j < KEY_RANGE; j++) {
                assertEquals(i, backupAccessor.get(i));
            }
        }
    }

    @Test
    public void test_whenEntryIsUpdatedBackupIsNotCleaned() {
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(new HazelcastExpiryPolicy(THREE_SECONDS, THREE_SECONDS, Duration.ETERNAL));
        Cache<Integer, Integer> cache = createCache(cacheConfig);

        for (int i = 0; i < KEY_RANGE; i++) {
            cache.put(i, i);
            cache.put(i, i);
        }

        cache.put(1, 1);
        sleepAtLeastSeconds(3);

        for (int i = 1; i < CLUSTER_SIZE; i++) {
            BackupAccessor backupAccessor = TestBackupUtils.newCacheAccessor(instances, cache.getName(), i);
            for (int j = 0; j < KEY_RANGE; j++) {
                assertEquals(i, backupAccessor.get(i));
            }
        }
    }

    @Test
    public void test_whenEntryIsRemovedBackupIsCleaned() {
        int ttlSeconds = 3;
        Duration duration = new Duration(TimeUnit.SECONDS, ttlSeconds);
        HazelcastExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(duration, duration, duration);
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(expiryPolicy);
        Cache<Integer, Integer> cache = createCache(cacheConfig);

        for (int i = 0; i < KEY_RANGE; i++) {
            cache.put(i, i);
        }

        for (int i = 0; i < KEY_RANGE; i++) {
            cache.remove(i, i);
        }

        for (int i = 1; i < CLUSTER_SIZE; i++) {
            BackupAccessor backupAccessor = TestBackupUtils.newCacheAccessor(instances, cache.getName(), i);
            assertBackupSizeEventually(0, backupAccessor);
        }
    }

    @Test
    public void test_whenEntryIsRemovedBackupIsCleaned_eternalDuration() {
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(new HazelcastExpiryPolicy(Duration.ETERNAL,
                Duration.ETERNAL, Duration.ETERNAL));
        Cache<Integer, Integer> cache = createCache(cacheConfig);

        for (int i = 0; i < KEY_RANGE; i++) {
            cache.put(i, i);
            cache.remove(i);
        }

        for (int i = 1; i < CLUSTER_SIZE; i++) {
            BackupAccessor backupAccessor = TestBackupUtils.newCacheAccessor(instances, cache.getName(), i);
            assertBackupSizeEventually(0, backupAccessor);
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
