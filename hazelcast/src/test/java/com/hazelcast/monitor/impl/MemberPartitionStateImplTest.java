package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberPartitionStateImplTest {

    private MemberPartitionStateImpl memberPartitionState;

    @Before
    public void setUp() {
        memberPartitionState = new MemberPartitionStateImpl();

        memberPartitionState.setMemberStateSafe(true);
        memberPartitionState.setMigrationQueueSize(125342);
        memberPartitionState.getPartitions().add(5);
        memberPartitionState.getPartitions().add(18);
    }

    @Test
    public void testDefaultConstructor() {
        assertTrue(memberPartitionState.isMemberStateSafe());
        assertEquals(125342, memberPartitionState.getMigrationQueueSize());
        assertNotNull(memberPartitionState.getPartitions());
        assertEquals(2, memberPartitionState.getPartitions().size());
        assertNotNull(memberPartitionState.toString());
    }

    @Test
    public void testSerialization() {
        JsonObject serialized = memberPartitionState.toJson();
        MemberPartitionStateImpl deserialized = new MemberPartitionStateImpl();
        deserialized.fromJson(serialized);

        assertTrue(deserialized.isMemberStateSafe());
        assertEquals(125342, deserialized.getMigrationQueueSize());
        assertNotNull(deserialized.getPartitions());
        assertEquals(2, deserialized.getPartitions().size());
        assertNotNull(deserialized.toString());
    }
}
