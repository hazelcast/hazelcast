package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LocalExecutorStatsImplTest {

    @Test
    public void testDefaultConstructor() {
        LocalExecutorStatsImpl localExecutorStats = new LocalExecutorStatsImpl();

        assertTrue(localExecutorStats.getCreationTime() > 0);
        assertEquals(0, localExecutorStats.getPendingTaskCount());
        assertEquals(0, localExecutorStats.getStartedTaskCount());
        assertEquals(0, localExecutorStats.getCompletedTaskCount());
        assertEquals(0, localExecutorStats.getCancelledTaskCount());
        assertEquals(0, localExecutorStats.getTotalStartLatency());
        assertEquals(0, localExecutorStats.getTotalExecutionLatency());
        assertNotNull(localExecutorStats.toString());
    }

    @Test
    public void testSerialization() {
        LocalExecutorStatsImpl localExecutorStats = new LocalExecutorStatsImpl();
        localExecutorStats.startPending();
        localExecutorStats.startPending();
        localExecutorStats.startPending();
        localExecutorStats.startPending();

        localExecutorStats.startExecution(5);
        localExecutorStats.startExecution(5);
        localExecutorStats.finishExecution(5);

        localExecutorStats.rejectExecution();
        localExecutorStats.cancelExecution();

        JsonObject serialized = localExecutorStats.toJson();
        LocalExecutorStatsImpl deserialized = new LocalExecutorStatsImpl();
        deserialized.fromJson(serialized);

        assertEquals(1, deserialized.getPendingTaskCount());
        assertEquals(2, deserialized.getStartedTaskCount());
        assertEquals(1, deserialized.getCompletedTaskCount());

        assertEquals(1, localExecutorStats.getCancelledTaskCount());
        assertEquals(1, deserialized.getCancelledTaskCount());
    }
}
