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

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.nio.DataReader;
import com.hazelcast.internal.nio.DataWriter;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.VersionAware;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.SerializationServiceSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;

import static com.hazelcast.test.ReflectionsHelper.REFLECTIONS;
import static com.hazelcast.test.ReflectionsHelper.filterNonConcreteClasses;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Iterates over all {@link DataSerializable} and {@link IdentifiedDataSerializable} classes
 * and checks if they have to implement {@link Versioned}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("WeakerAccess")
public class DataSerializableImplementsVersionedTest {

    private InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
    private Set<Class<? extends IdentifiedDataSerializable>> idsClasses;
    private Set<Class<? extends DataSerializable>> dsClasses;

    @Before
    public void setUp() {
        idsClasses = REFLECTIONS.getSubTypesOf(IdentifiedDataSerializable.class);
        filterNonConcreteClasses(idsClasses);

        dsClasses = REFLECTIONS.getSubTypesOf(DataSerializable.class);
        filterNonConcreteClasses(dsClasses);
        dsClasses.removeAll(idsClasses);
    }

    @Test
    public void testIdentifiedDataSerializableForVersionedInterface() throws Exception {
        for (Class<? extends IdentifiedDataSerializable> idsClass : idsClasses) {
            System.out.println(idsClass.getSimpleName());

            IdentifiedDataSerializable identifiedDataSerializable = getInstance(idsClass);
            if (identifiedDataSerializable == null) {
                continue;
            }

            checkInstanceOfVersion(idsClass, identifiedDataSerializable);
        }
    }

    @Test
    public void testDataSerializableForVersionedInterface() throws Exception {
        for (Class<? extends DataSerializable> dsClass : dsClasses) {
            System.out.println(dsClass.getSimpleName());

            DataSerializable dataSerializable = getInstance(dsClass);
            if (dataSerializable == null) {
                continue;
            }

            checkInstanceOfVersion(dsClass, dataSerializable);
        }
    }

    private <C> C getInstance(Class<C> clazz) throws Exception {
        Constructor<C> constructor = getConstructor(clazz);
        if (constructor == null) {
            return null;
        }
        try {
            return constructor.newInstance();
        } catch (InvocationTargetException e) {
            return null;
        }
    }

    private <C> Constructor<C> getConstructor(Class<C> idsClass) {
        try {
            Constructor<C> constructor = idsClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor;
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private void checkInstanceOfVersion(Class<? extends DataSerializable> clazz, DataSerializable dataSerializable)
            throws Exception {
        boolean getVersionCalledOnWrite = isGetVersionCalledOnWrite(dataSerializable);
        boolean getVersionCalledOnRead = isGetVersionCalledOnRead(dataSerializable);

        if (getVersionCalledOnWrite) {
            assertTrue("Expected " + clazz.getName() + " to implement Versioned, since out.getVersion() is used",
                    dataSerializable instanceof Versioned);
        }
        if (getVersionCalledOnRead) {
            assertTrue("Expected " + clazz.getName() + " to implement Versioned, since in.getVersion() is used",
                    dataSerializable instanceof Versioned);
        }
    }

    private boolean isGetVersionCalledOnWrite(DataSerializable dataSerializable) throws IOException {
        ObjectDataOutput out = getObjectDataOutput();
        when(out.getVersion()).thenReturn(Versions.V4_0);

        try {
            dataSerializable.writeData(out);
        } catch (NullPointerException | UnsupportedOperationException | AssertionError ignored) {
        }

        return isGetVersionCalled(out);
    }

    private boolean isGetVersionCalledOnRead(DataSerializable dataSerializable) throws IOException {
        ObjectDataInput in = getObjectDataInput();
        when(in.getVersion()).thenReturn(Versions.V4_0);

        try {
            dataSerializable.readData(in);
        } catch (NullPointerException | UnsupportedOperationException
                | IllegalArgumentException | ArithmeticException ignored) {
        }

        return isGetVersionCalled(in);
    }

    private boolean isGetVersionCalled(VersionAware versionAware) {
        try {
            verify(versionAware, never()).getVersion();
        } catch (AssertionError e) {
            return true;
        }
        return false;
    }

    // overridden in EE
    protected ObjectDataOutput getObjectDataOutput() {
        ObjectDataOutput output = mock(ObjectDataOutput.class,
                withSettings().extraInterfaces(SerializationServiceSupport.class, DataWriter.class));
        when(((SerializationServiceSupport) output).getSerializationService())
                .thenReturn(serializationService);
        return output;
    }

    // overridden in EE
    protected ObjectDataInput getObjectDataInput() {
        ObjectDataInput input = mock(ObjectDataInput.class,
                withSettings().extraInterfaces(SerializationServiceSupport.class, DataReader.class));
        when(((SerializationServiceSupport) input).getSerializationService())
                .thenReturn(serializationService);
        return input;
    }
}
