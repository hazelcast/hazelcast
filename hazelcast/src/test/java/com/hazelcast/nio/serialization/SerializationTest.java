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

import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.executor.impl.CancellationOperation;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.SimpleMemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.UuidUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
    public void testGlobalSerializer() {
        SerializationConfig serializationConfig = new SerializationConfig().setGlobalSerializerConfig(
                new GlobalSerializerConfig().setImplementation(new StreamSerializer<DummyValue>() {
                    public void write(ObjectDataOutput out, DummyValue v) throws IOException {
                        out.writeUTF(v.s);
                        out.writeInt(v.k);
                    }

                    public DummyValue read(ObjectDataInput in) throws IOException {
                        return new DummyValue(in.readUTF(), in.readInt());
                    }

                    public int getTypeId() {
                        return 123;
                    }

                    public void destroy() {
                    }
                }));

        SerializationService ss1 = new SerializationServiceBuilder().setConfig(serializationConfig).build();
        DummyValue value = new DummyValue("test", 111);
        Data data = ss1.toData(value);
        assertNotNull(data);

        SerializationService ss2 = new SerializationServiceBuilder().setConfig(serializationConfig).build();
        Object o = ss2.toObject(data);
        assertEquals(value, o);
    }

    @Test
    public void test_callid_on_correct_stream_position() throws Exception {
        SerializationService serializationService = new SerializationServiceBuilder().build();
        CancellationOperation operation = new CancellationOperation(UuidUtil.buildRandomUuidString(), true);
        operation.setCallerUuid(UuidUtil.buildRandomUuidString());
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

        SerializationService ss1 = new SerializationServiceBuilder().setConfig(serializationConfig).build();
        Data data = ss1.toData(new SingletonValue());
        assertNotNull(data);

        SerializationService ss2 = new SerializationServiceBuilder().setConfig(serializationConfig).build();
        Object o = ss2.toObject(data);
        assertEquals(new SingletonValue(), o);
    }

    private static class SingletonValue {
        public boolean equals(Object obj) {
            return obj instanceof SingletonValue;
        }
    }

    @Test
    public void testNullData() {
        Data data = new Data();
        SerializationService ss = new SerializationServiceBuilder().build();
        assertNull(ss.toObject(data));
    }

    /**
     * issue #1265
     */
    @Test
    public void testSharedJavaSerialization() {
        SerializationService ss = new SerializationServiceBuilder().setEnableSharedObject(true).build();
        Data data = ss.toData(new Foo());
        Foo foo = (Foo) ss.toObject(data);

        assertTrue("Objects are not identical!", foo == foo.getBar().getFoo());
    }

    @Test
    public void testLinkedListSerialization() {
        SerializationService ss = new SerializationServiceBuilder().build();
        LinkedList linkedList = new LinkedList();
        linkedList.add(new SerializationConcurrencyTest.Person(35, 180, 100, "Orhan", null));
        linkedList.add(new SerializationConcurrencyTest.Person(12, 120, 60, "Osman", null));
        Data data = ss.toData(linkedList);
        LinkedList deserialized =  ss.toObject(data);
        assertTrue("Objects are not identical!", linkedList.equals(deserialized));
    }

    @Test
    public void testArrayListSerialization() {
        SerializationService ss = new SerializationServiceBuilder().build();
        ArrayList arrayList = new ArrayList();
        arrayList.add(new SerializationConcurrencyTest.Person(35, 180, 100, "Orhan", null));
        arrayList.add(new SerializationConcurrencyTest.Person(12, 120, 60, "Osman", null));
        Data data = ss.toData(arrayList);
        ArrayList deserialized =  ss.toObject(data);
        assertTrue("Objects are not identical!", arrayList.equals(deserialized));
    }

    @Test
    public void testArraySerialization() {
        SerializationService ss = new SerializationServiceBuilder().build();
        byte[] array = new byte[1024];
        new Random().nextBytes(array);
        Data data = ss.toData(array);
        byte[] deserialized =  ss.toObject(data);
        assertEquals(array, deserialized);
    }

    /**
     * issue #1265
     */
    @Test
    public void testUnsharedJavaSerialization() {
        SerializationService ss = new SerializationServiceBuilder().setEnableSharedObject(false).build();
        Data data = ss.toData(new Foo());
        Foo foo =  ss.toObject(data);

        Assert.assertFalse("Objects should not be identical!", foo == foo.getBar().getFoo());
    }

    @Test(expected = ExecutionException.class, timeout = 120000)
    public void testGithubIssue2509()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();

        UnDeserializable unDeserializable = new UnDeserializable(1);
        IExecutorService executorService = h1.getExecutorService("default");
        Issue2509Runnable task = new Issue2509Runnable(unDeserializable);
        Future<?> future = executorService.submitToMember(task, h2.getCluster().getLocalMember());
        future.get();
    }

    public static class Issue2509Runnable
            implements Callable<Integer>, DataSerializable {

        private UnDeserializable unDeserializable;

        public Issue2509Runnable() {
        }

        public Issue2509Runnable(UnDeserializable unDeserializable) {
            this.unDeserializable = unDeserializable;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {

            out.writeObject(unDeserializable);
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {

            unDeserializable = in.readObject();
        }

        @Override
        public Integer call() {
            return unDeserializable.foo;
        }
    }

    public static class UnDeserializable
            implements DataSerializable {

        private int foo;

        public UnDeserializable(int foo) {
            this.foo = foo;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {

            out.writeInt(foo);
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {

            foo = in.readInt();
        }
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

    @Test
    public void testMemberLeftException_usingMemberImpl() throws IOException, ClassNotFoundException {
        String uuid = UuidUtil.buildRandomUuidString();
        String host = "127.0.0.1";
        int port = 5000;

        Member member = new MemberImpl(new Address(host, port), false, uuid, null);

        testMemberLeftException(uuid, host, port, member);
    }

    @Test
    public void testMemberLeftException_usingSimpleMember() throws IOException, ClassNotFoundException {
        String uuid = UuidUtil.buildRandomUuidString();
        String host = "127.0.0.1";
        int port = 5000;

        Member member = new SimpleMemberImpl(uuid, new InetSocketAddress(host, port));
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
    }
}
