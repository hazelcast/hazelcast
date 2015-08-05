package com.hazelcast.query.impl.getters;

import com.hazelcast.query.impl.AttributeType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReflectionHelperTest {

    @Test
    public void test_getAttributeType() throws Exception {
        AttributeType attributeType = ReflectionHelper.getAttributeType(null, "test");

        assertNull(attributeType);
    }
}