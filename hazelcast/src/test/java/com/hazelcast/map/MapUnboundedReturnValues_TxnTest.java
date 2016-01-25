package com.hazelcast.map;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class MapUnboundedReturnValues_TxnTest extends MapUnboundedReturnValuesTestSupport {

    @Test
    public void testTxnMap_withException_SmallLimit_NoPrecheck() {
        runMapTxn(PARTITION_COUNT, CLUSTER_SIZE, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_INACTIVE);
    }

    @Test
    public void testTxnMap_withException_SmallLimit_Precheck() {
        runMapTxn(PARTITION_COUNT, CLUSTER_SIZE, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_ACTIVE);
    }
}
