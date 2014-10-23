package com.hazelcast.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.TestUtil.getHazelcastInstanceImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class TimedMemberStateTest extends HazelcastTestSupport {

    @Test
    public void testSerialization() throws InterruptedException {
        HazelcastInstance hz = createHazelcastInstance();
        TimedMemberStateFactory timedMemberStateFactory = new DefaultTimedMemberStateFactory(getHazelcastInstanceImpl(hz));

        TimedMemberState state = timedMemberStateFactory.createTimedMemberState();
        JsonObject json = state.toJson();

        TimedMemberState deserialized = new TimedMemberState();
        deserialized.fromJson(json);

        assertNotNull(deserialized);
        assertEquals(state, deserialized);
    }
}
