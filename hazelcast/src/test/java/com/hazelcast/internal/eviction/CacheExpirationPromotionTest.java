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

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.backup.BackupAccessor;
import com.hazelcast.test.backup.TestBackupUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.spi.CachingProvider;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.cache.impl.eviction.CacheClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.test.OverridePropertyRule.set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({ParallelJVMTest.class, QuickTest.class})
public class CacheExpirationPromotionTest extends HazelcastTestSupport {

    @Rule
    public final OverridePropertyRule overrideTaskSecondsRule = set(PROP_TASK_PERIOD_SECONDS, "1");

    private String cacheName = "test";
    private int nodeCount = 3;
    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance[] instances;

    protected CacheConfig getCacheConfig() {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setName(cacheName);
        cacheConfig.setBackupCount(1);
        return cacheConfig;
    }

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(nodeCount);
        instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = factory.newHazelcastInstance(getConfig());
        }
    }

    @Test
    public void promoted_replica_should_send_eviction_to_other_backup() {
        final CachingProvider provider = createServerCachingProvider(instances[0]);
        provider.getCacheManager().createCache(cacheName, getCacheConfig());
        HazelcastCacheManager cacheManager = (HazelcastServerCacheManager) provider.getCacheManager();

        final String keyOwnedByLastInstance = generateKeyOwnedBy(instances[nodeCount - 1]);

        ICache<String, String> cache = cacheManager.getCache(cacheName).unwrap(ICache.class);
        cache.put(keyOwnedByLastInstance, "dummyVal", new CreatedExpiryPolicy(new Duration(TimeUnit.SECONDS, 5)));

        final BackupAccessor<String, String> backupAccessor = TestBackupUtils.newCacheAccessor(instances, cacheName, 1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull(backupAccessor.get(keyOwnedByLastInstance));
            }
        });

        instances[nodeCount - 1].shutdown();

        // the backup replica became the primary, now the backup is the other node.
        // we check if the newly appointed replica sent expiration to backups
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, backupAccessor.size());
            }
        });
    }
}
