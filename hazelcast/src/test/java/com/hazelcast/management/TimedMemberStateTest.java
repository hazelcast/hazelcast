package com.hazelcast.management;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class TimedMemberStateTest extends HazelcastTestSupport {

    @Test
    public void testSerialization() throws InterruptedException {
        HazelcastInstance hz = createHazelcastInstanceFactory(1).newHazelcastInstance();
        SerializationService serializationService = getNode(hz).getSerializationService();

        TimedMemberState state =getNode(hz).getManagementCenterService().getTimedMemberState();

        Data data = serializationService.toData(state);
        TimedMemberState result = (TimedMemberState) serializationService.toObject(data);
        assertNotNull(result);
    }
}
