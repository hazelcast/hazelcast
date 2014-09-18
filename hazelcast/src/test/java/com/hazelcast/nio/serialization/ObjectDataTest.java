/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SerializationConcurrencyTest.Address;
import com.hazelcast.nio.serialization.SerializationConcurrencyTest.Person;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author mdogan 1/23/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ObjectDataTest {

    @Test
    public void testDataWriter() throws IOException {
        SerializationService ss = new SerializationServiceBuilder().
                setUseNativeByteOrder(false).setByteOrder(ByteOrder.BIG_ENDIAN).build();

        final Person person = new Person(111, 123L, 89.56d, "test-person",
                new Address("street", 987));

        final Data data1 = ss.toData(person);

        ObjectDataOutput out = ss.createObjectDataOutput(1024);
        data1.writeData(out);
        byte[] bytes1 = out.toByteArray();

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        DataAdapter dataAdapter = new DataAdapter(data1, ss.getPortableContext());
        assertTrue(dataAdapter.writeTo(buffer));

        assertEquals(bytes1.length, buffer.position());
        byte[] bytes2 = new byte[buffer.position()];
        buffer.flip();
        buffer.get(bytes2);
        assertEquals(bytes1.length, bytes2.length);
        assertTrue(Arrays.equals(bytes1, bytes2));

        buffer.flip();
        dataAdapter.reset();
        dataAdapter.readFrom(buffer);
        Data data2 = dataAdapter.getData();

        assertEquals(data1, data2);
    }

    @Test
    public void testDataStreamsBigEndian() throws IOException {
        testDataStreams(ByteOrder.BIG_ENDIAN, false);
    }

    @Test
    public void testDataStreamsLittleEndian() throws IOException {
        testDataStreams(ByteOrder.LITTLE_ENDIAN, false);
    }

    @Test
    public void testDataStreamsNativeOrder() throws IOException {
        testDataStreams(ByteOrder.nativeOrder(), false);
    }

    @Test
    public void testDataStreamsNativeOrderUsingUnsafe() throws IOException {
        testDataStreams(ByteOrder.nativeOrder(), true);
    }

    private void testDataStreams(ByteOrder byteOrder, boolean allowUnsafe) throws IOException {
        SerializationService ss = new SerializationServiceBuilder()
                .setUseNativeByteOrder(false).setAllowUnsafe(allowUnsafe).setByteOrder(byteOrder).build();

        final Person person = new Person(111, 123L, 89.56d, "test-person",
                new Address("street", 987));

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectDataOutput out = ss.createObjectDataOutputStream(bout, byteOrder);
        out.writeObject(person);
        byte[] data1 = bout.toByteArray();

        ObjectDataOutput out2 = ss.createObjectDataOutput(1024);
        out2.writeObject(person);
        byte[] data2 = out2.toByteArray();

        assertEquals(data1.length, data2.length);
        assertTrue(Arrays.equals(data1, data2));

        final ByteArrayInputStream bin = new ByteArrayInputStream(data2);
        final ObjectDataInput in = ss.createObjectDataInputStream(bin, byteOrder);
        final Person person1 = in.readObject();

        final ObjectDataInput in2 = ss.createObjectDataInput(data1);
        final Person person2 = in2.readObject();

        assertEquals(person, person1);
        assertEquals(person, person2);
    }
}
