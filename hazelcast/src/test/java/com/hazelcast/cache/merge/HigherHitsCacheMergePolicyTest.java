package com.hazelcast.cache.merge;

import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.merge.policy.HigherHitsCacheMergePolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HigherHitsCacheMergePolicyTest extends AbstractCacheMergePolicyTest {

    @Override
    protected CacheMergePolicy createCacheMergePolicy() {
        return new HigherHitsCacheMergePolicy();
    }

}
