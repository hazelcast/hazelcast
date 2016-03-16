package com.hazelcast.internal.serialization.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class FactoryIdHelperTest extends HazelcastTestSupport {


    @Test
    public void testPropWithValidNumber() throws Exception {
        String key = "hazelcast.test.prop";
        System.setProperty(key, "1");
        int factoryId = FactoryIdHelper.getFactoryId(key, 10);

        assertEquals(1, factoryId);

        assertUtilityConstructor(FactoryIdHelper.class);
    }

    @Test
    public void testPropWithInValidNumber() throws Exception {
        String key = "hazelcast.test.prop";
        System.setProperty(key, "NaN");
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
