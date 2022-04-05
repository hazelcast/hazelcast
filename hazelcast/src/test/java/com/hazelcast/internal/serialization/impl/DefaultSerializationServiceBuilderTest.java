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

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteOrder;

import static com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder.DEFAULT_BYTE_ORDER;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DefaultSerializationServiceBuilderTest {

    private final String BYTE_ORDER_OVERRRIDE_PROPERTY = "hazelcast.serialization.byteOrder";

    @Test
    public void test_byteOrderIsOverridden_whenLittleEndian() {
        System.setProperty(BYTE_ORDER_OVERRRIDE_PROPERTY, "LITTLE_ENDIAN");
        try {
            InternalSerializationService serializationService = getSerializationServiceBuilder().build();
            assertEquals(ByteOrder.LITTLE_ENDIAN, serializationService.getByteOrder());
        } finally {
            System.clearProperty(BYTE_ORDER_OVERRRIDE_PROPERTY);
        }
    }

    @Test
    public void test_byteOrderIsOverridden_whenBigEndian() {
        System.setProperty(BYTE_ORDER_OVERRRIDE_PROPERTY, "BIG_ENDIAN");
        try {
            InternalSerializationService serializationService = getSerializationServiceBuilder().build();
            assertEquals(ByteOrder.BIG_ENDIAN, serializationService.getByteOrder());
        } finally {
            System.clearProperty(BYTE_ORDER_OVERRRIDE_PROPERTY);
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
        System.setProperty(ClusterProperty.SERIALIZATION_VERSION.getName(), "127");
        try {
            getSerializationServiceBuilder().setVersion(Byte.MIN_VALUE).build();
        } finally {
            System.clearProperty(ClusterProperty.SERIALIZATION_VERSION.getName());
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

    @Test
    public void test_nullByteOrder() {
        String override = System.getProperty(BYTE_ORDER_OVERRRIDE_PROPERTY);
        System.clearProperty(BYTE_ORDER_OVERRRIDE_PROPERTY);
        try {
            InternalSerializationService serializationService = getSerializationServiceBuilder()
                    .setByteOrder(null).build();
            assertEquals(DEFAULT_BYTE_ORDER, serializationService.getByteOrder());
        } finally {
            if (override != null) {
                System.setProperty(BYTE_ORDER_OVERRRIDE_PROPERTY, override);
            }
        }
    }

    @Test
    public void test_useNativeByteOrder() {
        ByteOrder nativeOrder = ByteOrder.nativeOrder();
        InternalSerializationService serializationService = getSerializationServiceBuilder().setUseNativeByteOrder(true).build();
        assertEquals(nativeOrder, serializationService.getByteOrder());
    }

    protected SerializationServiceBuilder getSerializationServiceBuilder() {
        return new DefaultSerializationServiceBuilder();
    }
}
