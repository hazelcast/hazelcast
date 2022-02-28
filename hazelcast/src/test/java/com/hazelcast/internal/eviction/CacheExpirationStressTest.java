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
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.backup.BackupAccessor;
import com.hazelcast.test.backup.TestBackupUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryExpiredListener;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.cache.impl.eviction.CacheClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.backup.TestBackupUtils.assertBackupSizeEventually;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class CacheExpirationStressTest extends HazelcastTestSupport {

    @Rule
    public final OverridePropertyRule overrideTaskSecondsRule = set(PROP_TASK_PERIOD_SECONDS, "2");

    protected final String cacheName = "test";
    protected String cacheNameWithPrefix;

    private static final int CLUSTER_SIZE = 5;
    private static final int KEY_RANGE = 100000;

    private HazelcastInstance[] instances = new HazelcastInstance[CLUSTER_SIZE];
    private TestHazelcastInstanceFactory factory;

    private Random random = new Random();
    private final AtomicBoolean done = new AtomicBoolean();
    private final int DURATION_SECONDS = 60;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(CLUSTER_SIZE);
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            instances[i] = factory.newHazelcastInstance(getConfig());
        }
    }

    protected CacheConfig getCacheConfig() {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new HazelcastExpiryPolicy(1000, 1000, 1000)));
        cacheConfig.setName(cacheName);
        cacheConfig.setBackupCount(CLUSTER_SIZE - 1);
        return cacheConfig;
    }

    @Test
    public void test() throws InterruptedException {
        assertClusterSize(CLUSTER_SIZE, instances);
        List<Thread> list = new ArrayList<>();
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            CacheConfig cacheConfig = getCacheConfig();
            Cache cache = createServerCachingProvider(instances[i])
                    .getCacheManager().createCache(cacheName, cacheConfig);
            cacheNameWithPrefix = cache.getName();
            list.add(new Thread(new TestRunner(cache, done)));
        }

        for (Thread thread: list) {
            thread.start();
        }

        sleepAtLeastSeconds(DURATION_SECONDS);

        done.set(true);
        for (Thread thread: list) {
            thread.join();
        }

        assertRecords(instances);
    }

    protected void assertRecords(final HazelcastInstance[] instances) {
        for (int i = 1; i < instances.length; i++) {
            BackupAccessor backupAccessor = TestBackupUtils.newCacheAccessor(instances, cacheNameWithPrefix, i);
            assertBackupSizeEventually(0, backupAccessor);
        }
        for (int i = 0; i < instances.length; i++) {
            final int index = i;
            assertEqualsEventually(() -> instances[index].getCacheManager().getCache(cacheName).size(), 0);
        }
        instances[0].getCacheManager().getCache(cacheName).destroy();
    }

    protected void doOp(Cache cache) {
        int op = random.nextInt(3);
        int key = random.nextInt(KEY_RANGE);
        int val = random.nextInt(KEY_RANGE);
        switch (op) {
            case 0:
                cache.put(key, val);
                break;
            case 1:
                cache.remove(key);
                break;
            case 2:
                cache.get(key);
                break;
            default:
                cache.get(key);
                break;
        }
    }

    class TestRunner implements Runnable {
        private Cache cache;
        private AtomicBoolean done;
        private CacheEntryExpiredListener listener;

        TestRunner(Cache cache, AtomicBoolean done) {
            this.cache = cache;
            this.done = done;
        }

        @Override
        public void run() {
            while (!done.get()) {
                doOp(cache);
            }
        }
    }
}
