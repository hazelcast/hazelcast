package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.SampleIdentifiedDataSerializable;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ConstructorFunction;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ArrayDataSerializableFactoryTest {

    private ArrayDataSerializableFactory factoryWithNonNullArray;
    private ArrayDataSerializableFactory factoryWithNonNullZeroArray;

    @Before
    public void before() throws Exception {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructorFunctions = new ConstructorFunction[1];

        constructorFunctions[0] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SampleIdentifiedDataSerializable();
            }
        };

        factoryWithNonNullArray = new ArrayDataSerializableFactory(constructorFunctions);
        factoryWithNonNullZeroArray = new ArrayDataSerializableFactory(new ConstructorFunction[0]);
    }

    @Test
    @Ignore
    public void testCreate() throws Exception {
        assertNull(factoryWithNonNullArray.create(-1));
        assertNull(factoryWithNonNullZeroArray.create(-1));

        assertNotNull(factoryWithNonNullArray.create(0));
        assertNull(factoryWithNonNullZeroArray.create(0));
    }
}
