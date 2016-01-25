package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteOrder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class FactoryIdHelperTest extends HazelcastTestSupport{


    @Test
    public void testPropWithValidNumber() throws Exception {
        String key = "hazelcast.test.prop";
        System.setProperty(key,"1");
        int factoryId = FactoryIdHelper.getFactoryId(key, 10);

        assertEquals(1, factoryId);

        assertUtilityConstructor(FactoryIdHelper.class);
    }

    @Test
    public void testPropWithInValidNumber() throws Exception {
        String key = "hazelcast.test.prop";
        System.setProperty(key,"NaN");
        int factoryId = FactoryIdHelper.getFactoryId(key, 10);

        assertEquals(10, factoryId);
    }

    @Test
    public void testPropWithNullNumber() throws Exception {
        String key = "hazelcast.test.prop";
        System.clearProperty(key);
        int factoryId = FactoryIdHelper.getFactoryId(key, 10);

        assertEquals(10, factoryId);
    }



}
