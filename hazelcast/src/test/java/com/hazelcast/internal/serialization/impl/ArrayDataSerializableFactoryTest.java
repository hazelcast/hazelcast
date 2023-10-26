/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ArrayDataSerializableFactoryTest {
    @Test
    public void testCreate() {
        Supplier<IdentifiedDataSerializable>[] constructorFunctions = new Supplier[1];

        constructorFunctions[0] = () -> new SampleIdentifiedDataSerializable();

        ArrayDataSerializableFactory factory = new ArrayDataSerializableFactory(constructorFunctions);

        assertNull(factory.create(-1));
        assertNull(factory.create(1));
        assertThat(factory.create(0)).isInstanceOf(SampleIdentifiedDataSerializable.class);
    }

    @Test
    public void testCreateWithoutVersion() {
        Supplier<IdentifiedDataSerializable>[] constructorFunctions = new Supplier[1];
        Supplier<IdentifiedDataSerializable> function = mock(Supplier.class);
        constructorFunctions[0] = function;

        ArrayDataSerializableFactory factory = new ArrayDataSerializableFactory(constructorFunctions);
        factory.create(0);

        verify(function, times(1)).get();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionWithEmptyConstructorFunctions() {
        new ArrayDataSerializableFactory(new Supplier[0]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionWithNullConstructorFunctions() {
        new ArrayDataSerializableFactory(null);
    }
}
