package com.hazelcast.client.map;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class ClientMapUnboundReturnValues_TxnTest extends ClientMapUnboundReturnValuesTestSupport {

    @Test
    public void testClientTxnMap_withException_NoPreCheck() {
        runClientMapTestTxnWithException(PARTITION_COUNT, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_INACTIVE);
    }

    @Test
    public void testClientTxnMap_withException_PreCheck() {
        runClientMapTestTxnWithException(PARTITION_COUNT, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_ACTIVE);
    }

    @Test
    public void testClientTxnMap_withoutException_NoPreCheck() {
        runClientMapTestTxnWithoutException(PARTITION_COUNT, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_INACTIVE);
    }

    @Test
    public void testClientTxnMap_withoutException_PreCheck() {
        runClientMapTestTxnWithoutException(PARTITION_COUNT, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_ACTIVE);
    }
}
