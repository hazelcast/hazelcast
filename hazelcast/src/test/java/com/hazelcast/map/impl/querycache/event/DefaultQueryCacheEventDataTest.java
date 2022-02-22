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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DefaultQueryCacheEventDataTest {

    private static final Object KEY = new Object();
    private static final Object VALUE = new Object();

    private static final Object DESERIALIZED_KEY = new Object();
    private static final Object DESERIALIZED_VALUE = new Object();

    private static final Data DATA_KEY = mock(Data.class);
    private static final Data DATA_OLD_VALUE = mock(Data.class);
    private static final Data DATA_NEW_VALUE = mock(Data.class);

    private SerializationService serializationService;

    private DefaultQueryCacheEventData queryCacheEventData;
    private DefaultQueryCacheEventData queryCacheEventDataSameAttributes;

    private DefaultQueryCacheEventData queryCacheEventDataOtherSequence;
    private DefaultQueryCacheEventData queryCacheEventDataOtherEventType;
    private DefaultQueryCacheEventData queryCacheEventDataOtherPartitionId;
    private DefaultQueryCacheEventData queryCacheEventDataOtherKey;
    private DefaultQueryCacheEventData queryCacheEventDataOtherValue;
    private DefaultQueryCacheEventData queryCacheEventDataOtherDataKey;
    private DefaultQueryCacheEventData queryCacheEventDataOtherDataNewValue;
    private DefaultQueryCacheEventData queryCacheEventDataOtherDataOldValue;
    private DefaultQueryCacheEventData queryCacheEventDataOtherSerializationService;

    @Before
    public void setUp() throws Exception {
        serializationService = mock(SerializationService.class);
        when(serializationService.toObject(eq(DATA_KEY))).thenReturn(DESERIALIZED_KEY);
        when(serializationService.toObject(eq(DATA_NEW_VALUE))).thenReturn(DESERIALIZED_VALUE);

        queryCacheEventData = new DefaultQueryCacheEventData();
        queryCacheEventDataSameAttributes = new DefaultQueryCacheEventData();

        queryCacheEventDataOtherSequence = new DefaultQueryCacheEventData();
        queryCacheEventDataOtherSequence.setSequence(23);

        queryCacheEventDataOtherEventType = new DefaultQueryCacheEventData();
        queryCacheEventDataOtherEventType.setEventType(42);

        queryCacheEventDataOtherPartitionId = new DefaultQueryCacheEventData();
        queryCacheEventDataOtherPartitionId.setPartitionId(123);

        queryCacheEventDataOtherKey = new DefaultQueryCacheEventData();
        queryCacheEventDataOtherKey.setKey(KEY);

        queryCacheEventDataOtherValue = new DefaultQueryCacheEventData();
        queryCacheEventDataOtherValue.setValue(VALUE);

        queryCacheEventDataOtherDataKey = new DefaultQueryCacheEventData();
        queryCacheEventDataOtherDataKey.setDataKey(DATA_KEY);

        queryCacheEventDataOtherDataNewValue = new DefaultQueryCacheEventData();
        queryCacheEventDataOtherDataNewValue.setDataNewValue(DATA_NEW_VALUE);

        queryCacheEventDataOtherDataOldValue = new DefaultQueryCacheEventData();
        queryCacheEventDataOtherDataOldValue.setDataOldValue(DATA_OLD_VALUE);

        queryCacheEventDataOtherSerializationService = new DefaultQueryCacheEventData();
        queryCacheEventDataOtherSerializationService.setSerializationService(serializationService);
    }

    @Test
    public void testKey() {
        queryCacheEventData.setSerializationService(serializationService);

        assertNull(queryCacheEventData.getDataKey());
        assertNull(queryCacheEventData.getKey());

        queryCacheEventData.setDataKey(DATA_KEY);
        assertEquals(DATA_KEY, queryCacheEventData.getDataKey());
        assertEquals(DESERIALIZED_KEY, queryCacheEventData.getKey());

        queryCacheEventData.setKey(KEY);
        assertEquals(KEY, queryCacheEventData.getKey());
    }

    @Test
    public void testValue() {
        queryCacheEventData.setSerializationService(serializationService);

        assertNull(queryCacheEventData.getDataOldValue());
        assertNull(queryCacheEventData.getDataNewValue());
        assertNull(queryCacheEventData.getValue());

        queryCacheEventData.setDataOldValue(DATA_OLD_VALUE);
        assertEquals(DATA_OLD_VALUE, queryCacheEventData.getDataOldValue());
        assertNull(queryCacheEventData.getDataNewValue());
        assertNull(queryCacheEventData.getValue());

        queryCacheEventData.setDataNewValue(DATA_NEW_VALUE);
        assertEquals(DATA_OLD_VALUE, queryCacheEventData.getDataOldValue());
        assertEquals(DATA_NEW_VALUE, queryCacheEventData.getDataNewValue());
        assertEquals(DESERIALIZED_VALUE, queryCacheEventData.getValue());

        queryCacheEventData.setValue(VALUE);
        assertEquals(DATA_OLD_VALUE, queryCacheEventData.getDataOldValue());
        assertEquals(DATA_NEW_VALUE, queryCacheEventData.getDataNewValue());
        assertEquals(VALUE, queryCacheEventData.getValue());
    }

    @Test
    public void testGetCreationTime() {
        assertTrue(queryCacheEventData.getCreationTime() > 0);
    }

    @Test
    public void testSequence() {
        assertEquals(0, queryCacheEventData.getSequence());

        queryCacheEventData.setSequence(5123);
        assertEquals(5123, queryCacheEventData.getSequence());
    }

    @Test
    public void testPartitionId() {
        assertEquals(0, queryCacheEventData.getPartitionId());

        queryCacheEventData.setPartitionId(9981);
        assertEquals(9981, queryCacheEventData.getPartitionId());
    }

    @Test
    public void testEventType() {
        assertEquals(0, queryCacheEventData.getEventType());

        queryCacheEventData.setEventType(42);
        assertEquals(42, queryCacheEventData.getEventType());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetSource() {
        queryCacheEventData.getSource();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetMapName() {
        queryCacheEventData.getMapName();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetCaller() {
        queryCacheEventData.getCaller();
    }

    @Test
    public void testToString() {
        assertNotNull(queryCacheEventData.toString());
    }

    @Test
    public void testEquals() {
        assertEquals(queryCacheEventData, queryCacheEventData);
        assertEquals(queryCacheEventData, queryCacheEventDataSameAttributes);

        assertNotEquals(queryCacheEventData, null);
        assertNotEquals(queryCacheEventData, new Object());

        assertNotEquals(queryCacheEventData, queryCacheEventDataOtherSequence);
        assertNotEquals(queryCacheEventData, queryCacheEventDataOtherEventType);
        assertNotEquals(queryCacheEventData, queryCacheEventDataOtherPartitionId);

        assertNotEquals(queryCacheEventData, queryCacheEventDataOtherKey);
        assertNotEquals(queryCacheEventData, queryCacheEventDataOtherValue);

        assertNotEquals(queryCacheEventData, queryCacheEventDataOtherDataKey);
        assertNotEquals(queryCacheEventData, queryCacheEventDataOtherDataNewValue);
        assertNotEquals(queryCacheEventData, queryCacheEventDataOtherDataOldValue);

        assertNotEquals(queryCacheEventData, queryCacheEventDataOtherSerializationService);
    }

    @Test
    public void testHashCode() {
        assertEquals(queryCacheEventData.hashCode(), queryCacheEventData.hashCode());
        assertEquals(queryCacheEventData.hashCode(), queryCacheEventDataSameAttributes.hashCode());

        assumeDifferentHashCodes();
        assertNotEquals(queryCacheEventData.hashCode(), queryCacheEventDataOtherSequence.hashCode());
        assertNotEquals(queryCacheEventData.hashCode(), queryCacheEventDataOtherEventType.hashCode());
        assertNotEquals(queryCacheEventData.hashCode(), queryCacheEventDataOtherPartitionId.hashCode());

        assertNotEquals(queryCacheEventData.hashCode(), queryCacheEventDataOtherKey.hashCode());
        assertNotEquals(queryCacheEventData.hashCode(), queryCacheEventDataOtherValue.hashCode());

        assertNotEquals(queryCacheEventData.hashCode(), queryCacheEventDataOtherDataKey.hashCode());
        assertNotEquals(queryCacheEventData.hashCode(), queryCacheEventDataOtherDataNewValue.hashCode());
        assertNotEquals(queryCacheEventData.hashCode(), queryCacheEventDataOtherDataOldValue.hashCode());

        assertNotEquals(queryCacheEventData.hashCode(), queryCacheEventDataOtherSerializationService.hashCode());
    }

    @Test
    public void testCopyConstructor() throws Exception {
        DefaultQueryCacheEventData actual = new DefaultQueryCacheEventData();
        actual.setPartitionId(1);
        actual.setEventType(2);
        actual.setKey(3);
        actual.setValue(4);

        DefaultQueryCacheEventData copied = new DefaultQueryCacheEventData(actual);

        assertEquals(copied, actual);
    }
}
