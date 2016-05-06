package com.hazelcast.cache.eviction;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.maxsize.impl.EntryCountCacheMaxSizeChecker;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;
import java.util.concurrent.ConcurrentMap;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheEvictionPolicyComparatorTest extends BaseCacheEvictionPolicyComparatorTest {

    @Override
    protected CachingProvider createCachingProvider(HazelcastInstance instance) {
        return HazelcastServerCachingProvider.createCachingProvider(instance);
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
        int partitionCount = Integer.parseInt(GroupProperty.PARTITION_COUNT.getDefaultValue());
        int iterationCount =
                (EntryCountCacheMaxSizeChecker.calculateMaxPartitionSize(
                        EvictionConfig.DEFAULT_MAX_ENTRY_COUNT, partitionCount) * partitionCount) * 2;
        EvictionConfig evictionConfig =
                new EvictionConfig()
                        .setComparatorClassName(MyEvictionPolicyComparator.class.getName());
        do_test_evictionPolicyComparator(evictionConfig, iterationCount);
    }

    @Test
    public void test_evictionPolicyComparator_with_comparatorInstance_when_maxSizePolicy_is_entryCount() {
        int partitionCount = Integer.parseInt(GroupProperty.PARTITION_COUNT.getDefaultValue());
        int iterationCount =
                (EntryCountCacheMaxSizeChecker.calculateMaxPartitionSize(
                        EvictionConfig.DEFAULT_MAX_ENTRY_COUNT, partitionCount) * partitionCount) * 2;
        EvictionConfig evictionConfig =
                new EvictionConfig().setComparator(new MyEvictionPolicyComparator());
        do_test_evictionPolicyComparator(evictionConfig, iterationCount);
    }

}
