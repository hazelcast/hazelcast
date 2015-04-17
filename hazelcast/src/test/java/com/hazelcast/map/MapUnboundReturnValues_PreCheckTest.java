package com.hazelcast.map;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class MapUnboundReturnValues_PreCheckTest extends MapUnboundReturnValuesTestSupport {

    @Test
    public void testMapKeySet_SmallLimit() {
        // with a single node we enforce the pre-check to trigger, since all items are on the local node
        runMapFullTest(PARTITION_COUNT, 1, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_ACTIVE, KeyType.STRING);
    }

    @Test
    public void testMapKeySet_MediumLimit() {
        // with a single node we enforce the pre-check to trigger, since all items are on the local node
        runMapFullTest(PARTITION_COUNT, 1, MEDIUM_LIMIT, PRE_CHECK_TRIGGER_LIMIT_ACTIVE, KeyType.INTEGER);
    }
}
