package com.hazelcast.cache.config;

import com.hazelcast.config.CacheEvictionConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.internal.eviction.EvictableEntryView;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheEvictionConfigTest {

    @Test
    public void test_cacheEvictionConfig_shouldInheritConstructors_from_evictionConfig_correctly() {
        CacheEvictionConfig cacheEvictionConfig1 =
                new CacheEvictionConfig(1000, EvictionConfig.MaxSizePolicy.ENTRY_COUNT, EvictionPolicy.LFU);
        assertEquals(1000, cacheEvictionConfig1.getSize());
        assertEquals(EvictionConfig.MaxSizePolicy.ENTRY_COUNT, cacheEvictionConfig1.getMaximumSizePolicy());
        assertEquals(EvictionPolicy.LFU, cacheEvictionConfig1.getEvictionPolicy());
        assertNotNull(cacheEvictionConfig1.toString());

        CacheEvictionConfig cacheEvictionConfig2 =
                new CacheEvictionConfig(1000, CacheEvictionConfig.CacheMaxSizePolicy.ENTRY_COUNT, EvictionPolicy.LFU);
        assertEquals(1000, cacheEvictionConfig2.getSize());
        assertEquals(CacheEvictionConfig.CacheMaxSizePolicy.ENTRY_COUNT, cacheEvictionConfig2.getMaxSizePolicy());
        assertEquals(EvictionPolicy.LFU, cacheEvictionConfig2.getEvictionPolicy());
        assertNotNull(cacheEvictionConfig2.toString());

        CacheEvictionConfig cacheEvictionConfig3 =
                new CacheEvictionConfig(1000, EvictionConfig.MaxSizePolicy.ENTRY_COUNT, "myComparator");
        assertEquals(1000, cacheEvictionConfig3.getSize());
        assertEquals(EvictionConfig.MaxSizePolicy.ENTRY_COUNT, cacheEvictionConfig3.getMaximumSizePolicy());
        assertEquals("myComparator", cacheEvictionConfig3.getComparatorClassName());
        assertNotNull(cacheEvictionConfig3.toString());

        CacheEvictionConfig cacheEvictionConfig4 =
                new CacheEvictionConfig(1000, CacheEvictionConfig.CacheMaxSizePolicy.ENTRY_COUNT, "myComparator");
        assertEquals(1000, cacheEvictionConfig4.getSize());
        assertEquals(CacheEvictionConfig.CacheMaxSizePolicy.ENTRY_COUNT, cacheEvictionConfig4.getMaxSizePolicy());
        assertEquals("myComparator", cacheEvictionConfig4.getComparatorClassName());
        assertNotNull(cacheEvictionConfig4.toString());

        EvictionPolicyComparator comparator = new EvictionPolicyComparator() {
            @Override
            public int compare(EvictableEntryView e1, EvictableEntryView e2) {
                return 0;
            }
        };

        CacheEvictionConfig cacheEvictionConfig5 =
                new CacheEvictionConfig(1000, EvictionConfig.MaxSizePolicy.ENTRY_COUNT, comparator);
        assertEquals(1000, cacheEvictionConfig5.getSize());
        assertEquals(EvictionConfig.MaxSizePolicy.ENTRY_COUNT, cacheEvictionConfig5.getMaximumSizePolicy());
        assertEquals(comparator, cacheEvictionConfig5.getComparator());
        assertNotNull(cacheEvictionConfig5.toString());

        CacheEvictionConfig cacheEvictionConfig6 =
                new CacheEvictionConfig(1000, CacheEvictionConfig.CacheMaxSizePolicy.ENTRY_COUNT, comparator);
        assertEquals(1000, cacheEvictionConfig6.getSize());
        assertEquals(CacheEvictionConfig.CacheMaxSizePolicy.ENTRY_COUNT, cacheEvictionConfig6.getMaxSizePolicy());
        assertEquals(comparator, cacheEvictionConfig6.getComparator());
        assertNotNull(cacheEvictionConfig6.toString());
    }

    @Test
    public void test_cacheEvictionConfig_shouldInheritAttributes_from_evictionConfig_correctly() {
        CacheEvictionConfig cacheEvictionConfig = new CacheEvictionConfig();
        cacheEvictionConfig.setComparatorClassName("myComparator");
        cacheEvictionConfig.setEvictionPolicy(EvictionPolicy.LRU);

        assertEquals("myComparator", cacheEvictionConfig.getComparatorClassName());
        assertEquals(EvictionPolicy.LRU, cacheEvictionConfig.getEvictionPolicy());
        assertNotNull(cacheEvictionConfig.toString());

        CacheEvictionConfig cacheEvictionConfigReadOnly = cacheEvictionConfig.getAsReadOnly();

        assertNotNull(cacheEvictionConfigReadOnly);
        assertEquals("myComparator", cacheEvictionConfigReadOnly.getComparatorClassName());
        assertEquals(EvictionPolicy.LRU, cacheEvictionConfigReadOnly.getEvictionPolicy());
        assertNotNull(cacheEvictionConfigReadOnly.toString());
    }

    @Test
    public void test_cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig_correctly_when_maxSizePolicyIs_entryCount() {
        test_cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig_correctly(
                EvictionConfig.MaxSizePolicy.ENTRY_COUNT);
    }

    @Test
    public void test_cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig_correctly_when_maxSizePolicyIs_usedNativeMemorySize() {
        test_cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig_correctly(
                EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE);
    }

    @Test
    public void test_cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig_correctly_when_maxSizePolicyIs_usedNativeMemoryPercentage() {
        test_cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig_correctly(
                EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
    }

    @Test
    public void test_cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig_correctly_when_maxSizePolicyIs_freeNativeMemorySize() {
        test_cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig_correctly(
                EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE);
    }

    @Test
    public void test_cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig_correctly_when_maxSizePolicyIs_freeNativeMemoryPercentage() {
        test_cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig_correctly(
                EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE);
    }

    private void test_cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig_correctly(
            EvictionConfig.MaxSizePolicy maxSizePolicy) {
        CacheEvictionConfig.CacheMaxSizePolicy cacheMaxSizePolicy =
                CacheEvictionConfig.CacheMaxSizePolicy.fromMaxSizePolicy(maxSizePolicy);

        CacheEvictionConfig cacheEvictionConfig = new CacheEvictionConfig();
        cacheEvictionConfig.setMaxSizePolicy(cacheMaxSizePolicy);

        assertEquals(maxSizePolicy, cacheEvictionConfig.getMaxSizePolicy().toMaxSizePolicy());
    }

}
