///*
// * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.hazelcast.nio.serialization;
//
//import com.hazelcast.nio.ObjectDataInput;
//import com.hazelcast.nio.ObjectDataOutput;
//import com.hazelcast.test.HazelcastSerialClassRunner;
//import com.hazelcast.test.annotation.QuickTest;
//import org.junit.Assert;
//import org.junit.Test;
//import org.junit.experimental.categories.Category;
//import org.junit.runner.RunWith;
//
//import java.io.ByteArrayInputStream;
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.nio.ByteOrder;
//import java.util.Arrays;
//
//import static com.hazelcast.nio.serialization.SerializationConcurrencyTest.Address;
//import static com.hazelcast.nio.serialization.SerializationConcurrencyTest.FACTORY_ID;
//import static com.hazelcast.nio.serialization.SerializationConcurrencyTest.Person;
//import static com.hazelcast.nio.serialization.SerializationConcurrencyTest.PortableAddress;
//import static com.hazelcast.nio.serialization.SerializationConcurrencyTest.PortablePerson;
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//
///**
// * @author mdogan 1/23/13
// */
//@RunWith(HazelcastSerialClassRunner.class)
//@Category(QuickTest.class)
//public class ObjectDataTest {
//
//    private final Person person = new Person(111, 123L, 89.56d, "test-person", new Address("street", 987));
//
//    private final PortablePerson portablePerson = new PortablePerson(222, 456L, "portable-person",
//                                                             new PortableAddress("street", 567));
//
//    private SerializationServiceBuilder createSerializationServiceBuilder() {
//        final PortableFactory portableFactory = new PortableFactory() {
//            public Portable create(int classId) {
//                switch (classId) {
//                    case 1:
//                        return new PortablePerson();
//                    case 2:
//                        return new PortableAddress();
//                }
//                throw new IllegalArgumentException();
//            }
//        };
//        return new DefaultSerializationServiceBuilder().addPortableFactory(FACTORY_ID, portableFactory);
//    }
//
//    @Test
//    public void testDataWriter() throws IOException {
//        testDataWriter(person);
//    }
//
//    @Test
//    public void testDataWriterPortable() throws IOException {
//        testDataWriter(portablePerson);
//    }
//
//    private void testDataWriter(Object object) throws IOException {
//        SerializationService ss = createSerializationServiceBuilder().build();
//        final Data data1 = ss.toData(object);
////        int dataSize = ss.getDataSize(data1);
//
//        ObjectDataOutput out = ss.createObjectDataOutput(1024);
//        ss.writeData(out, data1);
//        // SerializationService.writeData() has one byte IS_NULL header
//        byte[] src = out.toByteArray();
////        Assert.assertEquals(dataSize + 1, src.length);
//
//        byte[] bytes1 = new byte[dataSize];
//        System.arraycopy(src, 1, bytes1, 0, dataSize);
//
//        ByteBuffer buffer = ByteBuffer.allocate(1024);
//        DataAdapter dataAdapter = new DataAdapter(data1, ss.getPortableContext());
//        assertTrue(dataAdapter.writeTo(buffer));
//
//        assertEquals(dataSize, buffer.position());
//        byte[] bytes2 = new byte[dataSize];
//        buffer.flip();
//        buffer.get(bytes2);
//        assertEquals(bytes1.length, bytes2.length);
//        assertTrue(Arrays.equals(bytes1, bytes2));
//
//        buffer.flip();
//        dataAdapter.reset();
//        dataAdapter.readFrom(buffer);
//        Data data2 = dataAdapter.getData();
//
//        Assert.assertEquals(data1, data2);
//    }
//
//    @Test
//    public void testDataStreamsBigEndian() throws IOException {
//        testDataStreams(person, ByteOrder.BIG_ENDIAN, false);
//    }
//
//    @Test
//    public void testDataStreamsLittleEndian() throws IOException {
//        testDataStreams(person, ByteOrder.LITTLE_ENDIAN, false);
//    }
//
//    @Test
//    public void testDataStreamsNativeOrder() throws IOException {
//        testDataStreams(person, ByteOrder.nativeOrder(), false);
//    }
//
//    @Test
//    public void testDataStreamsNativeOrderUsingUnsafe() throws IOException {
//        testDataStreams(person, ByteOrder.nativeOrder(), true);
//    }
//
//    private void testDataStreams(Object object, ByteOrder byteOrder, boolean allowUnsafe) throws IOException {
//        SerializationService ss = createSerializationServiceBuilder()
//                .setUseNativeByteOrder(false).setAllowUnsafe(allowUnsafe).setByteOrder(byteOrder).build();
//
//        ByteArrayOutputStream bout = new ByteArrayOutputStream();
//        ObjectDataOutput out = ss.createObjectDataOutputStream(bout);
//        out.writeObject(object);
//        byte[] data1 = bout.toByteArray();
//
//        ObjectDataOutput out2 = ss.createObjectDataOutput(1024);
//        out2.writeObject(object);
//        byte[] data2 = out2.toByteArray();
//
//        assertEquals(data1.length, data2.length);
//        assertTrue(Arrays.equals(data1, data2));
//
//        final ByteArrayInputStream bin = new ByteArrayInputStream(data2);
//        final ObjectDataInput in = ss.createObjectDataInputStream(bin);
//        final Object object1 = in.readObject();
//
//        final ObjectDataInput in2 = ss.createObjectDataInput(data1);
//        final Object object2 = in2.readObject();
//
//        Assert.assertEquals(object, object1);
//        Assert.assertEquals(object, object2);
//    }
//}
