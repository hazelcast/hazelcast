package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SingleIMapEventTest {

    private QueryCacheEventData queryCacheEventData;
    private SingleIMapEvent singleIMapEvent;

    @Before
    public void setUp() {
        queryCacheEventData = new DefaultQueryCacheEventData();

        singleIMapEvent = new SingleIMapEvent(queryCacheEventData);
    }

    @Test
    public void testGetEventData() {
        assertEquals(queryCacheEventData, singleIMapEvent.getEventData());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetMember() {
        singleIMapEvent.getMember();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetEventType() {
        singleIMapEvent.getEventType();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetName() {
        singleIMapEvent.getName();
    }
}
