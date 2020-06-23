/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
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
import org.junit.runners.Parameterized;

import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.eviction.CacheClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.test.OverridePropertyRule.set;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExtendedCacheExpirationTest extends CacheTestSupport {

    @Rule
    public final OverridePropertyRule overrideTaskSecondsRule = set(PROP_TASK_PERIOD_SECONDS,
            valueOf(Integer.MAX_VALUE));

    @Parameterized.Parameters(name = "useSyncBackups:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true},
                {false}
        });
    }

    @Parameterized.Parameter(0)
    public boolean useSyncBackups;

    private static final int CLUSTER_SIZE = 2;
    private final Duration THREE_SECONDS = new Duration(TimeUnit.SECONDS, 3);

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

    @Test
    public void test_backupOperationAppliesDefaultExpiryPolicy() {
        HazelcastExpiryPolicy defaultExpiryPolicy = new HazelcastExpiryPolicy(THREE_SECONDS,
                Duration.ZERO, Duration.ZERO);

        CacheConfig cacheConfig = createCacheConfig(defaultExpiryPolicy);
        final ICache cache = createCache(cacheConfig);

        final int keyCount = 100;

        for (int i = 0; i < keyCount; i++) {
            cache.put(i, i);
        }

        // Check if all backup entries have applied the default expiry policy
        for (int i = 1; i < CLUSTER_SIZE; i++) {
            BackupAccessor backupAccessor = TestBackupUtils.newCacheAccessor(instances, cache.getName(), i);
            for (int j = 0; j < keyCount; j++) {
                TestBackupUtils.assertExpirationTimeExistsEventually(j, backupAccessor);
            }
        }

        // terminate other nodes than number zero to cause backup promotion at the 0th member
        for (int i = 1; i < CLUSTER_SIZE; i++) {
            getNode(instances[i]).shutdown(true);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < keyCount; i++) {
                    assertNull(cache.get(i));
                }
            }
        });
    }

    protected <K, V, M extends Serializable & ExpiryPolicy> CacheConfig<K, V> createCacheConfig(M expiryPolicy) {
        CacheConfig<K, V> cacheConfig = new CacheConfig<K, V>();
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

}
