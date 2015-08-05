package com.hazelcast.map;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapUnboundedReturnValues_QuickTest extends MapUnboundedReturnValuesTestSupport {

    @Test
    public void testMapKeySet_SmallLimit_NoPreCheck() {
        runMapQuickTest(PARTITION_COUNT, 1, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_INACTIVE, KeyType.INTEGER);
    }

    @Test
    public void testMapKeySet_SmallLimit_PreCheck() {
        runMapQuickTest(PARTITION_COUNT, 1, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_ACTIVE, KeyType.INTEGER);
    }
}
