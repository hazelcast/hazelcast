package com.hazelcast.internal.serialization.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SerializationTest extends HazelcastTestSupport {

    @Test
    public void testPrivateConstructors() throws Exception {
        assertUtilityConstructor(FactoryIdHelper.class);
        assertUtilityConstructor(SerializationUtil.class);
        assertUtilityConstructor(JavaDefaultSerializers.class);
        assertUtilityConstructor(ConstantSerializers.class);
        assertUtilityConstructor(SerializationConstants.class);
    }
}
