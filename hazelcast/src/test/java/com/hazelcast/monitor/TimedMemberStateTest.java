package com.hazelcast.monitor;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.instance.TestUtil.getHazelcastInstanceImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class TimedMemberStateTest extends HazelcastTestSupport {

    @Test
    public void testCloneAndSerialization() throws InterruptedException, CloneNotSupportedException {
        Set<String> instanceNames = new HashSet<String>();
        instanceNames.add("topicStats");

        HazelcastInstance hz = createHazelcastInstance();
        TimedMemberStateFactory timedMemberStateFactory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hz));

        TimedMemberState state = timedMemberStateFactory.createTimedMemberState();
        state.setClusterName("ClusterName");
        state.setTime(1827731);
        state.setInstanceNames(instanceNames);

        JsonObject serialized = state.clone().toJson();
        TimedMemberState deserialized = new TimedMemberState();
        deserialized.fromJson(serialized);

        assertNotNull(deserialized);
        assertEquals("ClusterName", deserialized.getClusterName());
        assertEquals(1827731, deserialized.getTime());
        assertNotNull(deserialized.getInstanceNames());
        assertEquals(1, deserialized.getInstanceNames().size());
        assertTrue(deserialized.getInstanceNames().contains("topicStats"));
        assertNotNull(deserialized.getMemberState());
        assertNotNull(deserialized.toString());
    }
}
