package com.hazelcast.map;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class MapUnboundedReturnValues_StringKeyTest extends MapUnboundedReturnValuesTestSupport {

    @Test
    public void testMap_SmallLimit_StringKey() {
        runMapFullTest(PARTITION_COUNT, CLUSTER_SIZE, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_INACTIVE, KeyType.STRING);
    }

    @Test
    public void testMap_MediumLimit_StringKey() {
        runMapFullTest(PARTITION_COUNT, CLUSTER_SIZE, MEDIUM_LIMIT, PRE_CHECK_TRIGGER_LIMIT_INACTIVE, KeyType.STRING);
    }

    @Test
    public void testMap_LargeLimit_StringKey() {
        runMapFullTest(PARTITION_COUNT, CLUSTER_SIZE, LARGE_LIMIT, PRE_CHECK_TRIGGER_LIMIT_INACTIVE, KeyType.STRING);
    }
}
