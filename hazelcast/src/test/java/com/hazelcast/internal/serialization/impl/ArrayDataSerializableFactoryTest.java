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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.VersionAwareConstructorFunction;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ArrayDataSerializableFactoryTest {

    @Test
    public void testCreate() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructorFunctions = new ConstructorFunction[1];

        constructorFunctions[0] = arg -> new SampleIdentifiedDataSerializable();

        ArrayDataSerializableFactory factory = new ArrayDataSerializableFactory(constructorFunctions);

        assertNull(factory.create(-1));
        assertNull(factory.create(1));
        assertThat(factory.create(0), instanceOf(SampleIdentifiedDataSerializable.class));
    }

    @Test
    public void testCreateWithoutVersion() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructorFunctions = new ConstructorFunction[1];
        VersionAwareConstructorFunction function = mock(VersionAwareConstructorFunction.class);
        constructorFunctions[0] = function;

        ArrayDataSerializableFactory factory = new ArrayDataSerializableFactory(constructorFunctions);
        factory.create(0);

        verify(function, times(1)).createNew(0);
        verify(function, times(0)).createNew(eq(0), any(Version.class), any(Version.class));
    }

    @Test
    public void testCreateWithVersion() throws Exception {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructorFunctions = new ConstructorFunction[1];
        VersionAwareConstructorFunction function = mock(VersionAwareConstructorFunction.class);
        constructorFunctions[0] = function;

        ArrayDataSerializableFactory factory = new ArrayDataSerializableFactory(constructorFunctions);
        Version version = MemberVersion.of(3, 6, 0).asVersion();
        Version wanProtocolVersion = MemberVersion.of(1, 0, 0).asVersion();
        factory.create(0, version, wanProtocolVersion);

        verify(function, times(0)).createNew(0);
        verify(function, times(1)).createNew(0, version, wanProtocolVersion);
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
