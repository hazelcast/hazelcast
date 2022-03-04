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

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.cache.impl.maxsize.impl.EntryCountCacheEvictionChecker.calculateMaxPartitionSize;
import static com.hazelcast.config.EvictionConfig.DEFAULT_MAX_ENTRY_COUNT;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheEvictionPolicyComparatorTest extends AbstractCacheEvictionPolicyComparatorTest {

    @Override
    protected CachingProvider createCachingProvider(HazelcastInstance instance) {
        return createServerCachingProvider(instance);
    }

    @Override
    protected HazelcastInstance createInstance(Config config) {
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory();
        return instanceFactory.newHazelcastInstance(config);
    }

    @Override
    protected ConcurrentMap getUserContext(HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getUserContext();
    }

    @Test
    public void test_evictionPolicyComparator_with_comparatorClassName_when_maxSizePolicy_is_entryCount() {
        int partitionCount = Integer.parseInt(ClusterProperty.PARTITION_COUNT.getDefaultValue());
        int iterationCount = (calculateMaxPartitionSize(DEFAULT_MAX_ENTRY_COUNT, partitionCount) * partitionCount) * 2;
        EvictionConfig evictionConfig = new EvictionConfig().setComparatorClassName(MyEvictionPolicyComparator.class.getName());

        testEvictionPolicyComparator(evictionConfig, iterationCount);
    }

    @Test
    public void test_evictionPolicyComparator_with_comparatorInstance_when_maxSizePolicy_is_entryCount() {
        int partitionCount = Integer.parseInt(ClusterProperty.PARTITION_COUNT.getDefaultValue());
        int iterationCount = (calculateMaxPartitionSize(DEFAULT_MAX_ENTRY_COUNT, partitionCount) * partitionCount) * 2;
        EvictionConfig evictionConfig = new EvictionConfig().setComparator(new MyEvictionPolicyComparator());

        testEvictionPolicyComparator(evictionConfig, iterationCount);
    }
}
