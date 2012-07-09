/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.nio;

import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.FactoryImpl.ProxyKey;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.nio.serialization.SerializationHelper;
import com.hazelcast.nio.serialization.SerializerManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.*;
import java.util.Date;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;
import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.*;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class SerializationTest {

    final private SerializerManager serializerManager = new SerializerManager();

    @AfterClass
    public static void after() {
        ThreadContext.get().setCurrentSerializerManager(null);
    }

    @Before
    public void init() {
        ThreadContext.get().setCurrentSerializerManager(serializerManager);
    }

    @Test
    public void testLongValueIncrement() {
        Data zero = toData(0L);
        Data five = IOUtil.addDelta(zero, 5L);
        assertEquals(5L, toObject(five));
        Data minusThree = IOUtil.addDelta(five, -8L);
        assertEquals(-3L, toObject(minusThree));
        Data minusTwo = IOUtil.addDelta(minusThree, 1L);
        assertEquals(-2L, toObject(minusTwo));
        Data twenty = IOUtil.addDelta(minusThree, 23L);
        assertEquals(20L, toObject(twenty));
    }

    @Test
    public void testLongValueIncrementShort() throws Exception {
        Data zero = toData(0L);
        Data five = IOUtil.addDelta(zero, 5L);
        assertEquals(5L, toObject(five));
    }

    public static String getHexString(byte[] b) throws Exception {
        String result = "";
        for (int i = 0; i < b.length; i++) {
            result +=
                    Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1);
        }
        return result;
    }

    @Test(expected = HazelcastSerializationException.class)
    public void newNotSerializableException() {
        final SerializationHelper serializer = new SerializationHelper(serializerManager);
        final Object o = new Object();
        serializer.writeObject(o);
    }

    @Test
    public void newNullSerializer() {
        final SerializationHelper serializer = new SerializationHelper(serializerManager);
        final Object o = null;
        final Data data = serializer.writeObject(o);
        assertEquals(o, serializer.readObject(data));
    }

    @Test
    public void newStringSerializer() {
        final SerializationHelper serializer = new SerializationHelper(serializerManager);
        final String s = "newStringSerializer 2@Z";
        final Data data = serializer.writeObject(s);
        assertEquals(s, serializer.readObject(data));
    }

    @Test
    public void newDateSerializer() {
        final SerializationHelper serializer = new SerializationHelper(serializerManager);
        final Date date = new Date();
        final Data data = serializer.writeObject(date);
        assertEquals(date, serializer.readObject(data));
    }

    @Test
    public void newSerializerExternalizable() {
        final SerializationHelper serializer = new SerializationHelper(serializerManager);
        final ExternalizableImpl o = new ExternalizableImpl();
        o.s = "Gallaxy";
        o.v = 42;
        final Data data = serializer.writeObject(o);
        byte[] b = data.buffer;
        assertFalse(b.length == 0);
        assertFalse(o.readExternal);
        assertTrue(o.writeExternal);
        final ExternalizableImpl object = (ExternalizableImpl) serializer.readObject(data);
        assertNotNull(object);
        assertNotSame(o, object);
        assertEquals(o, object);
        assertTrue(object.readExternal);
        assertFalse(object.writeExternal);
    }

    @Test
    public void newSerializerDataSerializable() {
        final SerializationHelper serializer = new SerializationHelper(serializerManager);
        final DataSerializableImpl o = new DataSerializableImpl();
        o.s = "Gallaxy";
        o.v = 42;
        final Data data = serializer.writeObject(o);
        byte[] b = data.buffer;
        assertFalse(b.length == 0);
        assertFalse(o.readExternal);
        assertTrue(o.writeExternal);
        final DataSerializableImpl object = (DataSerializableImpl) serializer.readObject(data);
        assertNotNull(object);
        assertNotSame(o, object);
        assertEquals(o, object);
        assertTrue(object.readExternal);
        assertFalse(object.writeExternal);
    }

    @Test
    public void newSerializerProxyKey() {
        final SerializationHelper serializer = new SerializationHelper(serializerManager);
        final FactoryImpl.ProxyKey o = new ProxyKey("key", 15L);
        final Data data = serializer.writeObject(o);
        byte[] b = data.buffer;
        assertFalse(b.length == 0);
        final ProxyKey object = (ProxyKey) serializer.readObject(data);
        assertNotNull(object);
        assertNotSame(o, object);
        assertEquals(o, object);
    }

    @Test
    public void testPrimitiveArray() {
        SerializationHelper serializer = new SerializationHelper(serializerManager);
        int[] value = new int[]{1, 2, 3};
        byte[] data = serializer.toByteArray(value);
        int[] value2 = (int[]) serializer.toObject(data);
        assertArrayEquals(value, value2);
    }

    private static class ExternalizableImpl implements Externalizable {
        private int v;
        private String s;

        private boolean readExternal = false;
        private boolean writeExternal = false;

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (!(obj instanceof ExternalizableImpl)) return false;
            final ExternalizableImpl other = (ExternalizableImpl) obj;
            return this.v == other.v &&
                    ((this.s == null && other.s == null) ||
                            (this.s != null && this.s.equals(other.s)));
        }

        @Override
        public int hashCode() {
            return this.v + 31 * (s != null ? s.hashCode() : 0);
        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
            v = in.readInt();
            s = in.readUTF();
            readExternal = true;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(v);
            out.writeUTF(s);
            writeExternal = true;
        }
    }

    private static class DataSerializableImpl implements DataSerializable {
        private int v;
        private String s;

        private boolean readExternal = false;
        private boolean writeExternal = false;

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (!(obj instanceof DataSerializableImpl)) return false;
            final DataSerializableImpl other = (DataSerializableImpl) obj;
            return this.v == other.v &&
                    ((this.s == null && other.s == null) ||
                            (this.s != null && this.s.equals(other.s)));
        }

        @Override
        public int hashCode() {
            return this.v + 31 * (s != null ? s.hashCode() : 0);
        }

        public void readData(DataInput in) throws IOException {
            v = in.readInt();
            s = in.readUTF();
            readExternal = true;
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeInt(v);
            out.writeUTF(s);
            writeExternal = true;
        }
    }
}
