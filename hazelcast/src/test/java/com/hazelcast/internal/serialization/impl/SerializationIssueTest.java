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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.SimpleMemberImpl;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.defaultserializers.JavaDefaultSerializers.JavaSerializer;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SerializationIssueTest extends HazelcastTestSupport {

    @Test
    public void testGlobalSerializer_withOverrideJavaSerializable() {
        GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig();
        globalSerializerConfig.setOverrideJavaSerialization(true);
        final AtomicInteger writeCounter = new AtomicInteger();
        final AtomicInteger readCounter = new AtomicInteger();
        final JavaSerializer javaSerializer = new JavaSerializer(true, false, null);
        SerializationConfig serializationConfig = new SerializationConfig().setGlobalSerializerConfig(
                globalSerializerConfig.setImplementation(new StreamSerializer<Object>() {
                    @Override
                    public void write(ObjectDataOutput out, Object v) throws IOException {
                        writeCounter.incrementAndGet();
                        if (v instanceof Serializable) {
                            out.writeBoolean(true);
                            javaSerializer.write(out, v);
                        } else if (v instanceof DummyValue) {
                            out.writeBoolean(false);
                            out.writeString(((DummyValue) v).s);
                            out.writeInt(((DummyValue) v).k);
                        }
                    }

                    @Override
                    public Object read(ObjectDataInput in) throws IOException {
                        readCounter.incrementAndGet();
                        boolean java = in.readBoolean();
                        if (java) {
                            return javaSerializer.read(in);
                        }
                        return new DummyValue(in.readString(), in.readInt());
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
                    @Override
                    public void write(ObjectDataOutput out, Object v) throws IOException {
                        writeCounter.incrementAndGet();
                        out.writeString(((DummyValue) v).s);
                        out.writeInt(((DummyValue) v).k);
                    }

                    @Override
                    public Object read(ObjectDataInput in) throws IOException {
                        readCounter.incrementAndGet();
                        return new DummyValue(in.readString(), in.readInt());
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

    private static class DummyValue {
        String s;
        int k;

        private DummyValue(String s, int k) {
            this.s = s;
            this.k = k;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DummyValue that = (DummyValue) o;

            if (k != that.k) {
                return false;
            }
            if (s != null ? !s.equals(that.s) : that.s != null) {
                return false;
            }

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
                            @Override
                            public void write(ObjectDataOutput out, SingletonValue v) throws IOException {
                            }

                            @Override
                            public SingletonValue read(ObjectDataInput in) throws IOException {
                                return new SingletonValue();
                            }

                            @Override
                            public int getTypeId() {
                                return 123;
                            }

                            @Override
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

        @Override
        public boolean equals(Object obj) {
            return obj instanceof SingletonValue;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
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
    public void testSynchronousQueueSerialization() {
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        SynchronousQueue q = new SynchronousQueue();
        Data data = ss.toData(q);
        SynchronousQueue deserialized = ss.toObject(data);
        assertTrue("Collections are not identical!", q.containsAll(deserialized));
        assertTrue("Collections are not identical!", deserialized.containsAll(q));
        assertEquals("Collection classes are not identical!", q.getClass(), deserialized.getClass());
    }

    @Test
    public void testArraySerialization() {
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        byte[] array = new byte[1024];
        new Random().nextBytes(array);
        Data data = ss.toData(array);
        byte[] deserialized = ss.toObject(data);
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

         Foo() {
            this.bar = new Bar();
        }

        public Bar getBar() {
            return bar;
        }

        private class Bar implements Serializable {
            public SerializationIssueTest.Foo getFoo() {
                return SerializationIssueTest.Foo.this;
            }
        }
    }

    /**
     * Ensures that SerializationService correctly handles compressed Serializables,
     * using a Properties object as a test case.
     */
    @Test
    public void testCompressionOnSerializables() {
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setEnableCompression(true)
                .build();
        long key = 1;
        long value = 5000;
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
    public void testCompressionOnExternalizables() {
        SerializationService serializationService = new DefaultSerializationServiceBuilder()
                .setEnableCompression(true)
                .build();
        String test = "test";
        ExternalizableString ex = new ExternalizableString(test);
        Data data = serializationService.toData(ex);

        ExternalizableString actual = serializationService.toObject(data);
        assertEquals(test, actual.value);
    }

    private static class ExternalizableString implements Externalizable {

        String value;

        ExternalizableString() {
        }

        ExternalizableString(String value) {
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
    public void testMemberLeftException_usingMemberImpl() throws Exception {
        UUID uuid = UuidUtil.newUnsecureUUID();
        String host = "127.0.0.1";
        int port = 5000;

        Member member = new MemberImpl.Builder(new Address(host, port))
                .version(MemberVersion.of("3.8.0"))
                .uuid(uuid).build();

        testMemberLeftException(uuid, host, port, member);
    }

    @Test
    public void testMemberLeftException_usingSimpleMember() throws Exception {
        UUID uuid = UuidUtil.newUnsecureUUID();
        String host = "127.0.0.1";
        int port = 5000;

        Member member = new SimpleMemberImpl(MemberVersion.of("3.8.0"), uuid, new InetSocketAddress(host, port));
        testMemberLeftException(uuid, host, port, member);
    }

    @Test
    public void testMemberLeftException_withLiteMemberImpl() throws Exception {
        UUID uuid = UuidUtil.newUnsecureUUID();
        String host = "127.0.0.1";
        int port = 5000;

        Member member = new MemberImpl.Builder(new Address(host, port))
                .version(MemberVersion.of("3.8.0"))
                .liteMember(true)
                .uuid(uuid)
                .build();

        testMemberLeftException(uuid, host, port, member);
    }

    @Test
    public void testMemberLeftException_withLiteSimpleMemberImpl() throws Exception {
        UUID uuid = UuidUtil.newUnsecureUUID();
        String host = "127.0.0.1";
        int port = 5000;

        Member member = new SimpleMemberImpl(MemberVersion.of("3.8.0"), uuid, new InetSocketAddress(host, port), true);
        testMemberLeftException(uuid, host, port, member);
    }

    private void testMemberLeftException(UUID uuid, String host, int port, Member member) throws Exception {

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
        assertEquals(member.getVersion(), member2.getVersion());
    }

    @Test
    public void testInternallySupportedClassExtended() {
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        TheClassThatExtendArrayList obj = new TheClassThatExtendArrayList();
        Data data = ss.toData(obj);
        Object obj2 = ss.toObject(data);

        assertEquals(obj2.getClass(), TheClassThatExtendArrayList.class);
    }

    static class TheClassThatExtendArrayList<E> extends ArrayList<E> implements DataSerializable {
        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(size());
            for (Object item : this) {
                out.writeObject(item);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public void readData(ObjectDataInput in) throws IOException {
            int size = in.readInt();
            for (int k = 0; k < size; k++) {
                add((E) in.readObject());
            }
        }
    }

    @Test
    public void testDynamicProxySerialization_withConfiguredClassLoader() {
        ClassLoader current = getClass().getClassLoader();
        DynamicProxyTestClassLoader cl = new DynamicProxyTestClassLoader(current);
        SerializationService ss = new DefaultSerializationServiceBuilder().setClassLoader(cl).build();
        IObjectA oa = (IObjectA) Proxy.newProxyInstance(current, new Class[]{IObjectA.class}, DummyInvocationHandler.INSTANCE);
        Data data = ss.toData(oa);
        Object o = ss.toObject(data);
        Assert.assertSame("configured classloader is not used", cl, o.getClass().getClassLoader());
        try {
            IObjectA.class.cast(o);
            Assert.fail("the serialized object should not be castable");
        } catch (ClassCastException expected) {
            // expected
        }
    }

    @Test
    public void testDynamicProxySerialization_withContextClassLoader() {
        ClassLoader oldContextLoader = Thread.currentThread().getContextClassLoader();
        try {
            ClassLoader current = getClass().getClassLoader();
            DynamicProxyTestClassLoader cl = new DynamicProxyTestClassLoader(current);
            Thread.currentThread().setContextClassLoader(cl);
            SerializationService ss = new DefaultSerializationServiceBuilder().setClassLoader(cl).build();
            IObjectA oa
                    = (IObjectA) Proxy.newProxyInstance(current, new Class[]{IObjectA.class}, DummyInvocationHandler.INSTANCE);
            Data data = ss.toData(oa);
            Object o = ss.toObject(data);
            Assert.assertSame("context classloader is not used", cl, o.getClass().getClassLoader());
            try {
                IObjectA.class.cast(o);
                Assert.fail("the serialized object should not be castable");
            } catch (ClassCastException expected) {
                // expected
            }
        } finally {
            Thread.currentThread().setContextClassLoader(oldContextLoader);
        }
    }

    @Test
    public void testNonPublicDynamicProxySerialization_withClassLoaderMess() {
        ClassLoader current = getClass().getClassLoader();
        DynamicProxyTestClassLoader cl1 = new DynamicProxyTestClassLoader(current, IPrivateObjectB.class.getName());
        DynamicProxyTestClassLoader cl2 = new DynamicProxyTestClassLoader(cl1, IPrivateObjectC.class.getName());
        SerializationService ss = new DefaultSerializationServiceBuilder().setClassLoader(cl2).build();
        Object ocd
                = Proxy.newProxyInstance(current, new Class[]{IPrivateObjectB.class, IPrivateObjectC.class}, DummyInvocationHandler.INSTANCE);
        Data data = ss.toData(ocd);
        try {
            ss.toObject(data);
            Assert.fail("the object should not be deserializable");
        } catch (IllegalAccessError expected) {
            // expected
        }
    }

    @Test
    public void testVersionedDataSerializable_outputHasMemberVersion() {
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        VersionedDataSerializable object = new VersionedDataSerializable();
        ss.toData(object);
        assertEquals("ObjectDataOutput.getVersion should be equal to member version",
                Version.of(BuildInfoProvider.getBuildInfo().getVersion()), object.getVersion());
    }

    @Test
    public void testVersionedDataSerializable_inputHasMemberVersion() {
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        VersionedDataSerializable object = new VersionedDataSerializable();
        VersionedDataSerializable otherObject = ss.toObject(ss.toData(object));
        assertEquals("ObjectDataInput.getVersion should be equal to member version",
                Version.of(BuildInfoProvider.getBuildInfo().getVersion()), otherObject.getVersion());
    }

    @Test
    public void testUuidSerializer() {
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        Random random = new Random();
        UUID uuid = new UUID(random.nextLong(), random.nextLong());
        assertEquals(uuid, ss.toObject(ss.toData(uuid)));
    }

    private static final class DynamicProxyTestClassLoader extends ClassLoader {

        private static final Set<String> WELL_KNOWN_TEST_CLASSES = new HashSet<String>(asList(IObjectA.class.getName(),
                IPrivateObjectB.class.getName(), IPrivateObjectC.class.getName()));

        private final Set<String> wellKnownClasses = new HashSet<String>();

        private DynamicProxyTestClassLoader(ClassLoader parent, String... classesToLoad) {
            super(parent);
            if (classesToLoad.length == 0) {
                wellKnownClasses.addAll(WELL_KNOWN_TEST_CLASSES);
            } else {
                wellKnownClasses.addAll(asList(classesToLoad));
            }
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!WELL_KNOWN_TEST_CLASSES.contains(name)) {
                return super.loadClass(name, resolve);
            }
            synchronized (this) {
                // first, check if the class has already been loaded
                Class<?> c = findLoadedClass(name);
                if (c == null) {
                    c = findClass(name);
                }
                if (resolve) {
                    resolveClass(c);
                }
                return c;
            }
        }

        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            if (!wellKnownClasses.contains(name)) {
                return super.findClass(name);
            }
            String path = name.replace('.', '/') + ".class";
            InputStream in = null;
            try {
                in = getParent().getResourceAsStream(path);
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                byte[] buf = new byte[1024];
                int read;
                while ((read = in.read(buf)) != -1) {
                    bout.write(buf, 0, read);
                }
                byte[] code = bout.toByteArray();
                return defineClass(name, code, 0, code.length);
            } catch (IOException e) {
                return super.findClass(name);
            } finally {
                IOUtil.closeResource(in);
            }
        }
    }

    public static final class DummyInvocationHandler implements InvocationHandler, Serializable {

        private static final long serialVersionUID = 3459316091095397098L;

        private static final DummyInvocationHandler INSTANCE = new DummyInvocationHandler();

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return null;
        }
    }

    @SuppressWarnings("unused")
    public interface IObjectA {
        void doA();
    }

    @SuppressWarnings("unused")
    interface IPrivateObjectB {
        void doC();
    }

    @SuppressWarnings("unused")
    interface IPrivateObjectC {
        void doD();
    }
}
