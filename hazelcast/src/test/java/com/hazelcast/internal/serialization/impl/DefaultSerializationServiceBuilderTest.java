/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DefaultSerializationServiceBuilderTest {

    @Test
    public void test_byteOrderIsOverridden_whenLittleEndian() {
        System.setProperty("hazelcast.serialization.byteOrder", "LITTLE_ENDIAN");
        try {
            InternalSerializationService serializationService = getSerializationServiceBuilder().build();
            assertEquals(ByteOrder.LITTLE_ENDIAN, serializationService.getByteOrder());
        } finally {
            System.clearProperty("hazelcast.serialization.byteOrder");
        }
    }

    @Test
    public void test_byteOrderIsOverridden_whenBigEndian() {
        System.setProperty("hazelcast.serialization.byteOrder", "BIG_ENDIAN");
        try {
            InternalSerializationService serializationService = getSerializationServiceBuilder().build();
            assertEquals(ByteOrder.BIG_ENDIAN, serializationService.getByteOrder());
        } finally {
            System.clearProperty("hazelcast.serialization.byteOrder");
        }
    }

    @Test
    public void test_versionResetToDefault_whenVersionNegative() {
        InternalSerializationService serializationService = getSerializationServiceBuilder()
                .setVersion(Byte.MIN_VALUE).build();
        assertEquals(BuildInfoProvider.getBuildInfo().getSerializationVersion(), serializationService.getVersion());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_exceptionThrown_whenNegativeVersionOverriddenViaProperty() {
        System.setProperty(GroupProperty.SERIALIZATION_VERSION.getName(), "127");
        try {
            getSerializationServiceBuilder().setVersion(Byte.MIN_VALUE).build();
        } finally {
            System.clearProperty(GroupProperty.SERIALIZATION_VERSION.getName());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_exceptionThrown_whenVersionGreaterThanMax() {
        getSerializationServiceBuilder().setVersion(Byte.MAX_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_exceptionThrown_whenPortableVersionNegative() {
        getSerializationServiceBuilder().setPortableVersion(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_exceptionThrown_whenInitialOutputBufferSizeNegative() {
        getSerializationServiceBuilder().setInitialOutputBufferSize(-1);
    }

    protected SerializationServiceBuilder getSerializationServiceBuilder() {
        return new DefaultSerializationServiceBuilder();
    }
}
