package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.SampleIdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * ArrayDataSerializableFactory Tester.
 *
 */
public class ArrayDataSerializableFactoryTest {

    private ArrayDataSerializableFactory factoryWithNonNullArray;
    private ArrayDataSerializableFactory factoryWithNonNullZeroArray;

    @Before
    public void before() throws Exception {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] ctorArray = new ConstructorFunction[1];

        ctorArray[0] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SampleIdentifiedDataSerializable();
            }
        };

        factoryWithNonNullArray = new ArrayDataSerializableFactory(ctorArray);
        factoryWithNonNullZeroArray = new ArrayDataSerializableFactory(new ConstructorFunction[0]);
    }

    @Test
    public void testCreate() throws Exception {
        assertNull(factoryWithNonNullArray.create(-1));
        assertNull(factoryWithNonNullZeroArray.create(-1));

        assertNotNull(factoryWithNonNullArray.create(0));
        assertNull(factoryWithNonNullZeroArray.create(0));
    }

} 
