package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.SampleIdentifiedDataSerializable;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ConstructorFunction;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ArrayDataSerializableFactoryTest {

    @Test
    public void testCreate() throws Exception {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructorFunctions = new ConstructorFunction[1];

        constructorFunctions[0] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SampleIdentifiedDataSerializable();
            }
        };

        ArrayDataSerializableFactory factory = new ArrayDataSerializableFactory(constructorFunctions);

        assertNull(factory.create(-1));
        assertNull(factory.create(1));
        assertThat(factory.create(0), instanceOf(SampleIdentifiedDataSerializable.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionWithEmptyConstructorFunctions() {
        new ArrayDataSerializableFactory(new ConstructorFunction[0]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionWithNullConstructorFunctions() {
        new ArrayDataSerializableFactory(null);
    }
}
