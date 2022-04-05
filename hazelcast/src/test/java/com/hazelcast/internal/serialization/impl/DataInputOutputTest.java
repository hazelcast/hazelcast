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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.createObjectDataInputStream;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.createObjectDataOutputStream;
import static com.hazelcast.internal.serialization.impl.SerializationConcurrencyTest.Address;
import static com.hazelcast.internal.serialization.impl.SerializationConcurrencyTest.Person;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DataInputOutputTest {

    private final Person person = new Person(111, 123L, 89.56d, "test-person", new Address("street", 987));

    @Test
    public void testDataStreamsBigEndian() throws IOException {
        testDataStreams(person, ByteOrder.BIG_ENDIAN, false);
    }

    @Test
    public void testDataStreamsLittleEndian() throws IOException {
        testDataStreams(person, ByteOrder.LITTLE_ENDIAN, false);
    }

    @Test
    public void testDataStreamsNativeOrder() throws IOException {
        testDataStreams(person, ByteOrder.nativeOrder(), false);
    }

    @Test
    public void testDataStreamsNativeOrderUsingUnsafe() throws IOException {
        testDataStreams(person, ByteOrder.nativeOrder(), true);
    }

    private void testDataStreams(Object object, ByteOrder byteOrder, boolean allowUnsafe) throws IOException {
        InternalSerializationService ss = createSerializationServiceBuilder()
                .setUseNativeByteOrder(false)
                .setAllowUnsafe(allowUnsafe)
                .setByteOrder(byteOrder)
                .build();

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectDataOutput out = createObjectDataOutputStream(bout, ss);
        out.writeObject(object);
        byte[] data1 = bout.toByteArray();

        ObjectDataOutput out2 = ss.createObjectDataOutput(1024);
        out2.writeObject(object);
        byte[] data2 = out2.toByteArray();

        assertEquals(data1.length, data2.length);
        assertTrue(Arrays.equals(data1, data2));

        final ByteArrayInputStream bin = new ByteArrayInputStream(data2);
        final ObjectDataInput in = createObjectDataInputStream(bin, ss);
        final Object object1 = in.readObject();

        final ObjectDataInput in2 = ss.createObjectDataInput(data1);
        final Object object2 = in2.readObject();

        Assert.assertEquals(object, object1);
        Assert.assertEquals(object, object2);
    }

    // overridden in EE
    protected SerializationServiceBuilder createSerializationServiceBuilder() {
        return new DefaultSerializationServiceBuilder();
    }
}
