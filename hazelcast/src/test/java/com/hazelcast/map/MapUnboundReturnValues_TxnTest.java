package com.hazelcast.map;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class MapUnboundReturnValues_TxnTest extends MapUnboundReturnValuesTestSupport {

    @Test
    public void testTxnMap_withException_SmallLimit_NoPrecheck() {
        runMapTxnWithExceptionTest(PARTITION_COUNT, CLUSTER_SIZE, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_INACTIVE);
    }

    @Test
    public void testTxnMap_withException_SmallLimit_Precheck() {
        runMapTxnWithExceptionTest(PARTITION_COUNT, CLUSTER_SIZE, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_ACTIVE);
    }

    @Test
    public void testTxnMap_withoutException_SmallLimit_NoPrecheck() {
        runMapTxnWithoutExceptionTest(PARTITION_COUNT, CLUSTER_SIZE, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_INACTIVE);
    }

    @Test
    public void testTxnMap_withoutException_SmallLimit_Precheck() {
        runMapTxnWithoutExceptionTest(PARTITION_COUNT, CLUSTER_SIZE, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_ACTIVE);
    }
}
