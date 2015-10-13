package com.hazelcast.client.map;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class ClientMapUnboundReturnValues_TxnTest extends ClientMapUnboundReturnValuesTestSupport {

    @Test
    public void testClientTxnMap_withException_NoPreCheck() {
        runClientMapTestTxn(PARTITION_COUNT, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_INACTIVE);
    }

    @Test
    public void testClientTxnMap_withException_PreCheck() {
        runClientMapTestTxn(PARTITION_COUNT, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_ACTIVE);
    }
}
