package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LocalCacheWideEventDataTest extends HazelcastTestSupport {

    private LocalCacheWideEventData localCacheWideEventData;

    @Before
    public void setUp() {
        localCacheWideEventData = new LocalCacheWideEventData("source", 23, 42);
    }

    @Test
    public void testGetNumberOfEntriesAffected() {
        assertEquals(42, localCacheWideEventData.getNumberOfEntriesAffected());
    }

    @Test
    public void testGetSource() {
        assertEquals("source", localCacheWideEventData.getSource());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetMapName() {
        localCacheWideEventData.getMapName();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetCaller() {
        localCacheWideEventData.getCaller();
    }

    @Test
    public void testGetEventType() {
        assertEquals(23, localCacheWideEventData.getEventType());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWriteData() throws Exception {
        localCacheWideEventData.writeData(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReadData() throws Exception {
        localCacheWideEventData.readData(null);
    }

    @Test
    public void testToString() {
        assertContains(localCacheWideEventData.toString(), "LocalCacheWideEventData");
    }
}
