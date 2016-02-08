/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.executor.impl.operations.CancellationOperation;
import com.hazelcast.internal.instance.MemberImpl;
import com.hazelcast.internal.instance.SimpleMemberImpl;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.JavaDefaultSerializers;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.UuidUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author mdogan 30/10/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SerializationTest
        extends HazelcastTestSupport {

    @Test
    public void testGlobalSerializer_withOverrideJavaSerializable() {
        GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig();
        globalSerializerConfig.setOverrideJavaSerialization(true);
        final AtomicInteger writeCounter = new AtomicInteger();
        final AtomicInteger readCounter = new AtomicInteger();
        final JavaDefaultSerializers.JavaSerializer javaSerializer = new JavaDefaultSerializers.JavaSerializer(true,false);
        SerializationConfig serializationConfig = new SerializationConfig().setGlobalSerializerConfig(
                globalSerializerConfig.setImplementation(new StreamSerializer<Object>() {
                    public void write(ObjectDataOutput out, Object v) throws IOException {
                        writeCounter.incrementAndGet();
                        if(v instanceof Serializable){
                            out.writeBoolean(true);
                            javaSerializer.write(out, v);
                        } else if(v instanceof DummyValue){
                            out.writeBoolean(false);
                            out.writeUTF(((DummyValue)v).s);
                            out.writeInt(((DummyValue)v).k);
                        }
                    }

                    public Object read(ObjectDataInput in) throws IOException {
                        readCounter.incrementAndGet();
                        boolean java = in.readBoolean();
                        if(java) {
                            return javaSerializer.read(in);
                        }
                        return new DummyValue(in.readUTF(), in.readInt());
                    }

                    public int getTypeId() {
                        return 123;
                    }

                    public void destroy() {
                    }
                }));

        SerializationService ss1 = new DefaultSerializationServiceBuilder().setConfig(serializationConfig).build();
        DummyValue value = new DummyValue("test", 111);
        Data data1 = ss1.toData(value);
        Data data2 = ss1.toData(new Foo());
        Assert.assertNotNull(data1);
        Assert.assertNotNull(data2);
        assertEquals(2, writeCounter.get());

        SerializationService ss2 = new DefaultSerializationServiceBuilder().setConfig(serializationConfig).build();
        Object o1 = ss2.toObject(data1);
        Object o2 = ss2.toObject(data2);
        Assert.assertEquals(value, o1);
        Assert.assertNotNull(o2);
        assertEquals(2, readCounter.get());
    }

    @Test
    public void testGlobalSerializer_withoutOverrideJavaSerializable() {
        GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig();
        globalSerializerConfig.setOverrideJavaSerialization(false);
        final AtomicInteger writeCounter = new AtomicInteger();
        final AtomicInteger readCounter = new AtomicInteger();
        SerializationConfig serializationConfig = new SerializationConfig().setGlobalSerializerConfig(
                globalSerializerConfig.setImplementation(new StreamSerializer<Object>() {
                    public void write(ObjectDataOutput out, Object v) throws IOException {
                        writeCounter.incrementAndGet();
                        out.writeUTF(((DummyValue) v).s);
                        out.writeInt(((DummyValue) v).k);
                    }

                    public Object read(ObjectDataInput in) throws IOException {
                        readCounter.incrementAndGet();
                        return new DummyValue(in.readUTF(), in.readInt());
                    }

                    public int getTypeId() {
                        return 123;
                    }

                    public void destroy() {
                    }
                }));

        SerializationService ss1 = new DefaultSerializationServiceBuilder().setConfig(serializationConfig).build();
        DummyValue value = new DummyValue("test", 111);
        Data data1 = ss1.toData(value);
        Data data2 = ss1.toData(new Foo());
        Assert.assertNotNull(data1);
        Assert.assertNotNull(data2);
        assertEquals(1, writeCounter.get());

        SerializationService ss2 = new DefaultSerializationServiceBuilder().setConfig(serializationConfig).build();
        Object o1 = ss2.toObject(data1);
        Object o2 = ss2.toObject(data2);
        Assert.assertEquals(value, o1);
        Assert.assertNotNull(o2);
        assertEquals(1, readCounter.get());
    }

    @Test
    public void test_callid_on_correct_stream_position() throws Exception {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        CancellationOperation operation = new CancellationOperation(UuidUtil.newUnsecureUuidString(), true);
        operation.setCallerUuid(UuidUtil.newUnsecureUuidString());
        OperationAccessor.setCallId(operation, 12345);

        Data data = serializationService.toData(operation);
        long callId = IOUtil.extractOperationCallId(data, serializationService);

        assertEquals(12345, callId);
    }

    private static class DummyValue {
        String s;
        int k;

        private DummyValue(String s, int k) {
            this.s = s;
            this.k = k;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DummyValue that = (DummyValue) o;

            if (k != that.k) return false;
            if (s != null ? !s.equals(that.s) : that.s != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = s != null ? s.hashCode() : 0;
            result = 31 * result + k;
            return result;
        }
    }

    @Test
    public void testEmptyData() {
        SerializationConfig serializationConfig = new SerializationConfig().addSerializerConfig(
                new SerializerConfig().setTypeClass(SingletonValue.class)
                        .setImplementation(new StreamSerializer<SingletonValue>() {
                            public void write(ObjectDataOutput out, SingletonValue v) throws IOException {
                            }

                            public SingletonValue read(ObjectDataInput in) throws IOException {
                                return new SingletonValue();
                            }

                            public int getTypeId() {
                                return 123;
                            }

                            public void destroy() {
                            }
                        }));

        SerializationService ss1 = new DefaultSerializationServiceBuilder().setConfig(serializationConfig).build();
        Data data = ss1.toData(new SingletonValue());
        Assert.assertNotNull(data);

        SerializationService ss2 = new DefaultSerializationServiceBuilder().setConfig(serializationConfig).build();
        Object o = ss2.toObject(data);
        Assert.assertEquals(new SingletonValue(), o);
    }

    private static class SingletonValue {
        public boolean equals(Object obj) {
            return obj instanceof SingletonValue;
        }
    }

    @Test
    public void testNullData() {
        Data data = new HeapData();
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        assertNull(ss.toObject(data));
    }

    /**
     * issue #1265
     */
    @Test
    public void testSharedJavaSerialization() {
        SerializationService ss = new DefaultSerializationServiceBuilder().setEnableSharedObject(true).build();
        Data data = ss.toData(new Foo());
        Foo foo = (Foo) ss.toObject(data);

        assertTrue("Objects are not identical!", foo == foo.getBar().getFoo());
    }

    @Test
    public void testLinkedListSerialization() {
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        LinkedList linkedList = new LinkedList();
        linkedList.add(new SerializationConcurrencyTest.Person(35, 180, 100, "Orhan", null));
        linkedList.add(new SerializationConcurrencyTest.Person(12, 120, 60, "Osman", null));
        Data data = ss.toData(linkedList);
        LinkedList deserialized = ss.toObject(data);
        assertTrue("Objects are not identical!", linkedList.equals(deserialized));
    }

    @Test
    public void testArrayListSerialization() {
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        ArrayList arrayList = new ArrayList();
        arrayList.add(new SerializationConcurrencyTest.Person(35, 180, 100, "Orhan", null));
        arrayList.add(new SerializationConcurrencyTest.Person(12, 120, 60, "Osman", null));
        Data data = ss.toData(arrayList);
        ArrayList deserialized = ss.toObject(data);
        assertTrue("Objects are not identical!", arrayList.equals(deserialized));
    }

    @Test
    public void testArraySerialization() {
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        byte[] array = new byte[1024];
        new Random().nextBytes(array);
        Data data = ss.toData(array);
        byte[] deserialized =  ss.toObject(data);
        assertArrayEquals(array, deserialized);
    }

    @Test
    public void testPartitionHash() {
        PartitioningStrategy partitionStrategy = new PartitioningStrategy() {
            @Override
            public Object getPartitionKey(Object key) {
                return key.hashCode();
            }
        };

        SerializationService ss = new DefaultSerializationServiceBuilder().build();

        String obj = String.valueOf(System.nanoTime());
        Data dataWithPartitionHash = ss.toData(obj, partitionStrategy);
        Data dataWithOutPartitionHash = ss.toData(obj);

        assertTrue(dataWithPartitionHash.hasPartitionHash());
        assertNotEquals(dataWithPartitionHash.hashCode(), dataWithPartitionHash.getPartitionHash());

        assertFalse(dataWithOutPartitionHash.hasPartitionHash());
        assertEquals(dataWithOutPartitionHash.hashCode(), dataWithOutPartitionHash.getPartitionHash());
    }

    /**
     * issue #1265
     */
    @Test
    public void testUnsharedJavaSerialization() {
        SerializationService ss = new DefaultSerializationServiceBuilder().setEnableSharedObject(false).build();
        Data data = ss.toData(new Foo());
        Foo foo = ss.toObject(data);

        Assert.assertFalse("Objects should not be identical!", foo == foo.getBar().getFoo());
    }

    private static class Foo implements Serializable {
        public Bar bar;

        public Foo() {
            this.bar = new Bar();
        }

        public Bar getBar() {
            return bar;
        }

        private class Bar implements Serializable {
            public Foo getFoo() {
                return Foo.this;
            }
        }
    }


    /**
     * Ensures that SerializationService correctly handles compressed Serializables,
     * using a Properties object as a test case.
     */
    @Test
    public void testCompressionOnSerializables() throws Exception {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().setEnableCompression(true).build();
        long key = 1, value = 5000;
        Properties properties = new Properties();
        properties.put(key, value);
        Data data = serializationService.toData(properties);
        
        Properties output = serializationService.toObject(data);
        assertEquals(value, output.get(key));
    }

    /**
     * Ensures that SerializationService correctly handles compressed Serializables,
     * using a test-specific object as a test case.
     */
    @Test
    public void testCompressionOnExternalizables() throws Exception {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().setEnableCompression(true).build();
        String test = "test";
        ExternalizableString ex = new ExternalizableString(test);  
        Data data = serializationService.toData(ex);
        
        ExternalizableString actual = serializationService.toObject(data);
        assertEquals(test, actual.value);
    }
    
    private static class ExternalizableString implements Externalizable {
        
        String value; 
        
        public ExternalizableString() {
            
        }
        
        public ExternalizableString(String value) {
            this.value = value;
        }
        
        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeUTF(value);            
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            value = in.readUTF();           
        }
        
    }
    
    @Test
    public void testMemberLeftException_usingMemberImpl() throws IOException, ClassNotFoundException {
        String uuid = UuidUtil.newUnsecureUuidString();
        String host = "127.0.0.1";
        int port = 5000;

        Member member = new MemberImpl(new Address(host, port), false, uuid, null);

        testMemberLeftException(uuid, host, port, member);
    }

    @Test
    public void testMemberLeftException_usingSimpleMember() throws IOException, ClassNotFoundException {
        String uuid = UuidUtil.newUnsecureUuidString();
        String host = "127.0.0.1";
        int port = 5000;

        Member member = new SimpleMemberImpl(uuid, new InetSocketAddress(host, port));
        testMemberLeftException(uuid, host, port, member);
    }

    @Test
    public void testMemberLeftException_withLiteMemberImpl() throws IOException, ClassNotFoundException {
        String uuid = UuidUtil.newUnsecureUuidString();
        String host = "127.0.0.1";
        int port = 5000;

        Member member = new MemberImpl(new Address(host, port), false, uuid, null, null, true);

        testMemberLeftException(uuid, host, port, member);
    }

    @Test
    public void testMemberLeftException_withLiteSimpleMemberImpl() throws IOException, ClassNotFoundException {
        String uuid = UuidUtil.newUnsecureUuidString();
        String host = "127.0.0.1";
        int port = 5000;

        Member member = new SimpleMemberImpl(uuid, new InetSocketAddress(host, port), true);
        testMemberLeftException(uuid, host, port, member);
    }

    private void testMemberLeftException(String uuid, String host, int port, Member member)
            throws IOException, ClassNotFoundException {

        MemberLeftException exception = new MemberLeftException(member);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bout);
        out.writeObject(exception);

        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        ObjectInputStream in = new ObjectInputStream(bin);
        MemberLeftException exception2 = (MemberLeftException) in.readObject();
        MemberImpl member2 = (MemberImpl) exception2.getMember();

        assertEquals(uuid, member2.getUuid());
        assertEquals(host, member2.getAddress().getHost());
        assertEquals(port, member2.getAddress().getPort());
        assertEquals(member.isLiteMember(), member2.isLiteMember());
    }

    @Test
    public void testInternallySupportedClassExtended() throws Exception {
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        TheClassThatExtendArrayList obj = new TheClassThatExtendArrayList();
        Data data = ss.toData(obj);
        Object obj2 = ss.toObject(data);

        assertEquals(obj2.getClass(), TheClassThatExtendArrayList.class);

    }

    static class TheClassThatExtendArrayList extends ArrayList implements DataSerializable {
        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(size());
            for (Object item : this) {
                out.writeObject(item);
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            int size = in.readInt();
            for (int k = 0; k < size; k++) {
                add(in.readObject());
            }
        }
    }

}
