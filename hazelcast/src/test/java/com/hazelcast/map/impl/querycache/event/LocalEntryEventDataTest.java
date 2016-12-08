package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LocalEntryEventDataTest extends HazelcastTestSupport {

    private InternalSerializationService serializationService;

    // passed params are type of OBJECT to this event
    private LocalEntryEventData objectEvent;

    // passed params are type of DATA to this event
    private LocalEntryEventData dataEvent;

    @Before
    public void before() {
        serializationService = new DefaultSerializationServiceBuilder().build();
        objectEvent = new LocalEntryEventData(serializationService, "source", 1, "key", "oldValue", "value", 1);
        dataEvent = new LocalEntryEventData(serializationService, "source", 1, toData("key"), toData("oldValue"), toData("value"), 1);
    }

    @After
    public void after() {
        serializationService.dispose();
    }

    @Test
    public void test_getValue_withDataValue() throws Exception {
        assertEquals("value", dataEvent.getValue());
    }

    @Test
    public void test_getValue_withObjectValue() throws Exception {
        assertEquals("value", objectEvent.getValue());
    }

    @Test
    public void test_getOldValue_withDataValue() throws Exception {
        assertEquals("oldValue", dataEvent.getOldValue());
    }

    @Test
    public void test_getOldValue_withObjectValue() throws Exception {
        assertEquals("oldValue", objectEvent.getOldValue());
    }

    @Test
    public void test_getValueData_withDataValue() throws Exception {
        assertEquals(toData("value"), dataEvent.getValueData());
    }

    @Test
    public void test_getValueData_withObjectValue() throws Exception {
        assertEquals(toData("value"), objectEvent.getValueData());
    }

    @Test
    public void test_getOldValueData_withDataValue() throws Exception {
        assertEquals(toData("oldValue"), dataEvent.getOldValueData());
    }

    @Test
    public void test_getOldValueData_withObjectValue() throws Exception {
        assertEquals(toData("oldValue"), objectEvent.getOldValueData());
    }

    private Data toData(Object obj) {
        return serializationService.toData(obj);
    }
}
