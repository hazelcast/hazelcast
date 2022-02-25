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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalEntryEventDataTest extends HazelcastTestSupport {

    private InternalSerializationService serializationService;

    // passed params are type of OBJECT to this event
    private LocalEntryEventData objectEvent;

    // passed params are type of DATA to this event
    private LocalEntryEventData dataEvent;

    @Before
    public void setUp() {
        serializationService = new DefaultSerializationServiceBuilder().build();
        objectEvent = new LocalEntryEventData(serializationService, "source", 23, "key", "oldValue", "value", 42);
        dataEvent = new LocalEntryEventData(serializationService, "source", 23, toData("key"), toData("oldValue"),
                toData("value"), 42);
    }

    @After
    public void tearDown() {
        serializationService.dispose();
    }

    @Test
    public void testGetValue_withDataValue() {
        assertEquals("value", dataEvent.getValue());
    }

    @Test
    public void testGetValue_withObjectValue() {
        assertEquals("value", objectEvent.getValue());
    }

    @Test
    public void testGetOldValue_withDataValue() {
        assertEquals("oldValue", dataEvent.getOldValue());
    }

    @Test
    public void testGetOldValue_withObjectValue() {
        assertEquals("oldValue", objectEvent.getOldValue());
    }

    @Test
    public void testGetKey_withDataKey() {
        assertEquals("key", dataEvent.getKey());
    }

    @Test
    public void testGetKey_withObjectKey() {
        assertEquals("key", objectEvent.getKey());
    }

    @Test
    public void testGetKeyData_withDataKey() {
        assertEquals(toData("key"), dataEvent.getKeyData());
    }

    @Test
    public void testGetKeyData_withObjectKey() {
        assertEquals(toData("key"), objectEvent.getKeyData());
    }

    @Test
    public void testGetValueData_withDataValue() {
        assertEquals(toData("value"), dataEvent.getValueData());
    }

    @Test
    public void testGetValueData_withObjectValue() {
        assertEquals(toData("value"), objectEvent.getValueData());
    }

    @Test
    public void testGetOldValueData_withDataValue() {
        assertEquals(toData("oldValue"), dataEvent.getOldValueData());
    }

    @Test
    public void testGetOldValueData_withObjectValue() {
        assertEquals(toData("oldValue"), objectEvent.getOldValueData());
    }

    @Test
    public void testGetSource() {
        assertEquals("source", dataEvent.getSource());
        assertEquals("source", objectEvent.getSource());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetMapName() {
        dataEvent.getMapName();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetCaller() {
        dataEvent.getCaller();
    }

    @Test
    public void testGetEventType() {
        assertEquals(23, dataEvent.getEventType());
        assertEquals(23, objectEvent.getEventType());
    }

    @Test
    public void testGetPartitionId() {
        assertEquals(42, dataEvent.getPartitionId());
        assertEquals(42, objectEvent.getPartitionId());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWriteData() throws Exception {
        dataEvent.writeData(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReadData() throws Exception {
        dataEvent.readData(null);
    }

    @Test
    public void testToString() {
        assertContains(dataEvent.toString(), "LocalEntryEventData");
    }

    private Data toData(Object obj) {
        return serializationService.toData(obj);
    }
}
