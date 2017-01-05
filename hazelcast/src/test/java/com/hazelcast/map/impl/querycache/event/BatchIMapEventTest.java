package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BatchIMapEventTest {

    private BatchEventData batchEventData;
    private BatchIMapEvent batchIMapEvent;

    @Before
    public void setUp() throws Exception {
        batchEventData = new BatchEventData(Collections.<QueryCacheEventData>emptyList(), "source", 1);

        batchIMapEvent = new BatchIMapEvent(batchEventData);
    }

    @Test
    public void testGetBatchEventData() {
        assertEquals(batchEventData, batchIMapEvent.getBatchEventData());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetMember() {
        batchIMapEvent.getMember();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetEventType() {
        batchIMapEvent.getEventType();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetName() {
        batchIMapEvent.getName();
    }
}
