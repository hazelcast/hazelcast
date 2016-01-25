package com.hazelcast.map.merge;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LatestUpdateMapMergePolicyTest extends AbstractMapMergePolicyTest {

    @Before
    public void given() {
        policy = new LatestUpdateMapMergePolicy();
    }

}
