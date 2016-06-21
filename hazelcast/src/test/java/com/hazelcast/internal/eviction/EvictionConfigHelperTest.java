package com.hazelcast.internal.eviction;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.internal.eviction.impl.EvictionConfigHelper;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EvictionConfigHelperTest {

    @Test
    public void test_privateConstructor_of_EvictionConfigHelper() {
        HazelcastTestSupport.assertUtilityConstructor(EvictionConfigHelper.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_fail_when_evictionConfig_is_null() {
        EvictionConfigHelper.checkEvictionConfig(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_fail_when_bothOf_comparator_and_comparatorClassName_are_set() {
        EvictionConfigHelper.checkEvictionConfig(EvictionPolicy.LRU, "myComparator", new Object());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_fail_when_noneOfThe_comparator_and_comparatorClassName_are_set_if_evictionPolicy_is_null() {
        EvictionConfigHelper.checkEvictionConfig(null, null, null);
    }

    @Test
    public void test_pass_when_comparatorClassName_is_set_if_evictionPolicy_is_null() {
        EvictionConfigHelper.checkEvictionConfig(null, "myComparator", null);
    }

    @Test
    public void test_pass_when_comparator_is_set_if_evictionPolicy_is_null() {
        EvictionConfigHelper.checkEvictionConfig(null, null, new Object());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_fail_when_noneOfThe_comparator_and_comparatorClassName_are_set_if_evictionPolicy_is_none() {
        EvictionConfigHelper.checkEvictionConfig(EvictionPolicy.NONE, null, null);
    }

    @Test
    public void test_pass_when_comparatorClassName_is_set_if_evictionPolicy_is_none() {
        EvictionConfigHelper.checkEvictionConfig(EvictionPolicy.NONE, "myComparator", null);
    }

    @Test
    public void test_pass_when_comparator_is_set_if_evictionPolicy_is_none() {
        EvictionConfigHelper.checkEvictionConfig(EvictionPolicy.NONE, null, new Object());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_fail_when_noneOfThe_comparator_and_comparatorClassName_are_set_if_evictionPolicy_is_random() {
        EvictionConfigHelper.checkEvictionConfig(EvictionPolicy.RANDOM, null, null);
    }

    @Test
    public void test_pass_when_comparatorClassName_is_set_if_evictionPolicy_is_random() {
        EvictionConfigHelper.checkEvictionConfig(EvictionPolicy.RANDOM, "myComparator", null);
    }

    @Test
    public void test_pass_when_comparator_is_set_if_evictionPolicy_is_random() {
        EvictionConfigHelper.checkEvictionConfig(EvictionPolicy.RANDOM, null, new Object());
    }

    @Test
    public void test_ok_when_comparatorClassName_is_set_if_evictionPolicy_is_notSet() {
        EvictionConfigHelper.checkEvictionConfig(EvictionConfig.DEFAULT_EVICTION_POLICY, "myComparator", null);
    }

    @Test
    public void test_ok_when_comparator_is_set_if_evictionPolicy_is_notSet() {
        EvictionConfigHelper.checkEvictionConfig(EvictionConfig.DEFAULT_EVICTION_POLICY, null, new Object());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_fail_when_comparatorClassName_is_set_if_evictionPolicy_is_set_also() {
        // Default eviction policy is `LRU`. See `EvictionConfig.DEFAULT_EVICTION_POLICY`
        EvictionConfigHelper.checkEvictionConfig(EvictionPolicy.LFU, "myComparator", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_fail_when_comparator_is_set_if_evictionPolicy_is_set_also() {
        // Default eviction policy is `LRU`. See `EvictionConfig.DEFAULT_EVICTION_POLICY`
        EvictionConfigHelper.checkEvictionConfig(EvictionPolicy.LFU, null, new Object());
    }

}
