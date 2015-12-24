package com.hazelcast.map.impl.record;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EmptyRecordStatisticsTest extends HazelcastTestSupport {

    EmptyRecordStatistics emptyRecordStatistics = new EmptyRecordStatistics();

    @Test
    public void testGetHits() throws Exception {
        assertEquals(0, emptyRecordStatistics.getHits());
    }

    @Test
    public void testSetHits() throws Exception {
        emptyRecordStatistics.setHits(1);
        assertEquals(0, emptyRecordStatistics.getHits());
    }

    @Test
    public void testGetExpirationTime() throws Exception {
        assertEquals(0, emptyRecordStatistics.getExpirationTime());
    }

    @Test
    public void testSetExpirationTime() throws Exception {
        emptyRecordStatistics.setExpirationTime(1);
        assertEquals(0, emptyRecordStatistics.getExpirationTime());
    }

    @Test
    public void testGetLastStoredTime() throws Exception {
        assertEquals(0, emptyRecordStatistics.getExpirationTime());
    }

    @Test
    public void testSetLastStoredTime() throws Exception {
        emptyRecordStatistics.setLastStoredTime(1);
        assertEquals(0, emptyRecordStatistics.getExpirationTime());
    }

    @Test
    public void testGetMemoryCost() throws Exception {
        assertEquals(0, emptyRecordStatistics.getMemoryCost());
    }
}