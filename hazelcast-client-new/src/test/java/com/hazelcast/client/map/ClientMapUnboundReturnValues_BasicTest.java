package com.hazelcast.client.map;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class ClientMapUnboundReturnValues_BasicTest extends ClientMapUnboundReturnValuesTestSupport {

    @Test
    public void testClientMap_withException_NoPreCheck() {
        runClientMapTestWithException(PARTITION_COUNT, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_INACTIVE);
    }

    @Test
    public void testClientMap_withException_PreCheck() {
        runClientMapTestWithException(PARTITION_COUNT, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_ACTIVE);
    }

    @Test
    public void testClientMap_withoutException_NoPreCheck() {
        runClientMapTestWithoutException(PARTITION_COUNT, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_INACTIVE);
    }

    @Test
    public void testClientMap_withoutException_PreCheck() {
        runClientMapTestWithoutException(PARTITION_COUNT, SMALL_LIMIT, PRE_CHECK_TRIGGER_LIMIT_ACTIVE);
    }

    @Test
    public void testClientMap_checkUnsupported() {
        runClientMapTestCheckUnsupported();
    }
}
