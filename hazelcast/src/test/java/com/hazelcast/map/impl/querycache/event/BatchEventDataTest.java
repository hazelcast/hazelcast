/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BatchEventDataTest extends HazelcastTestSupport {

    private QueryCacheEventData eventData;
    private QueryCacheEventData otherEventData;

    private BatchEventData batchEventData;
    private BatchEventData batchEventDataSameAttribute;

    private BatchEventData batchEventDataOtherSource;
    private BatchEventData batchEventDataOtherPartitionId;
    private BatchEventData batchEventDataOtherEvent;
    private BatchEventData batchEventDataNoEvent;

    @Before
    public void setUp() {
        eventData = new DefaultQueryCacheEventData();
        eventData.setSequence(1);
        otherEventData = new DefaultQueryCacheEventData();
        eventData.setSequence(2);

        ArrayList<QueryCacheEventData> events = new ArrayList<QueryCacheEventData>();
        events.add(eventData);

        batchEventData = new BatchEventData(events, "source", 1);
        batchEventDataSameAttribute = new BatchEventData(events, "source", 1);

        batchEventDataOtherSource = new BatchEventData(events, "otherSource", 1);
        batchEventDataOtherPartitionId = new BatchEventData(events, "source", 2);
        batchEventDataOtherEvent = new BatchEventData(singleton(otherEventData), "source", 1);
        batchEventDataNoEvent = new BatchEventData(Collections.<QueryCacheEventData>emptyList(), "source", 1);
    }

    @Test
    public void testAdd() {
        batchEventData.add(otherEventData);

        assertEquals(2, batchEventData.size());

        Collection<QueryCacheEventData> events = batchEventData.getEvents();
        assertContains(events, eventData);
        assertContains(events, otherEventData);
    }

    @Test
    public void testIsEmpty() {
        assertFalse(batchEventData.isEmpty());
        assertFalse(batchEventDataSameAttribute.isEmpty());
        assertFalse(batchEventDataOtherSource.isEmpty());
        assertFalse(batchEventDataOtherPartitionId.isEmpty());
        assertFalse(batchEventDataOtherEvent.isEmpty());
        assertTrue(batchEventDataNoEvent.isEmpty());
    }

    @Test
    public void testSize() {
        assertEquals(1, batchEventData.size());
        assertEquals(1, batchEventDataSameAttribute.size());
        assertEquals(1, batchEventDataOtherSource.size());
        assertEquals(1, batchEventDataOtherPartitionId.size());
        assertEquals(1, batchEventDataOtherEvent.size());
        assertEquals(0, batchEventDataNoEvent.size());
    }

    @Test
    public void testGetPartitionId() {
        assertEquals(1, batchEventData.getPartitionId());
        assertEquals(1, batchEventDataSameAttribute.getPartitionId());
        assertEquals(1, batchEventDataOtherSource.getPartitionId());
        assertEquals(2, batchEventDataOtherPartitionId.getPartitionId());
        assertEquals(1, batchEventDataOtherEvent.getPartitionId());
        assertEquals(1, batchEventDataNoEvent.getPartitionId());
    }

    @Test
    public void testGetSource() {
        assertEquals("source", batchEventData.getSource());
        assertEquals("source", batchEventDataSameAttribute.getSource());
        assertEquals("otherSource", batchEventDataOtherSource.getSource());
        assertEquals("source", batchEventDataOtherPartitionId.getSource());
        assertEquals("source", batchEventDataOtherEvent.getSource());
        assertEquals("source", batchEventDataNoEvent.getSource());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetMapName() {
        batchEventData.getMapName();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetCaller() {
        batchEventData.getCaller();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetEventType() {
        batchEventData.getEventType();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetSequence() {
        batchEventData.getSequence();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetSequence() {
        batchEventData.setSequence(1);
    }

    @Test
    public void testEquals() {
        assertEquals(batchEventData, batchEventData);
        assertEquals(batchEventData, batchEventDataSameAttribute);

        assertNotEquals(batchEventData, null);
        assertNotEquals(batchEventData, new Object());

        assertEquals(batchEventData, batchEventDataOtherSource);
        assertEquals(batchEventData, batchEventDataOtherPartitionId);
        assertNotEquals(batchEventData, batchEventDataOtherEvent);
        assertNotEquals(batchEventData, batchEventDataNoEvent);
    }

    @Test
    public void testHashCode() {
        assertEquals(batchEventData.hashCode(), batchEventData.hashCode());
        assertEquals(batchEventData.hashCode(), batchEventDataSameAttribute.hashCode());

        assertEquals(batchEventData.hashCode(), batchEventDataOtherSource.hashCode());
        assertEquals(batchEventData.hashCode(), batchEventDataOtherPartitionId.hashCode());
        assumeDifferentHashCodes();
        assertNotEquals(batchEventData.hashCode(), batchEventDataOtherEvent.hashCode());
        assertNotEquals(batchEventData.hashCode(), batchEventDataNoEvent.hashCode());
    }

    @Test
    public void testToString() {
        assertContains(batchEventData.toString(), "BatchEventData");
    }
}
