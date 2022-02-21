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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.internal.serialization.impl.TestSerializationConstants;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * MorphingPortableReader Tester.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MorphingPortableReaderTest {

    private SerializationServiceV1 service1;
    private SerializationServiceV1 service2;

    private PortableReader reader;

    @Before
    public void before() throws Exception {
        service1 = (SerializationServiceV1) new DefaultSerializationServiceBuilder()
                .addPortableFactory(TestSerializationConstants.PORTABLE_FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new MorphingBasePortable();
                    }

                }).build();

        service2 = (SerializationServiceV1) new DefaultSerializationServiceBuilder()
                .addPortableFactory(TestSerializationConstants.PORTABLE_FACTORY_ID, new PortableFactory() {
                    public Portable create(int classId) {
                        return new MorphingPortable();
                    }
                }).build();

        Data data = service1.toData(new MorphingBasePortable((byte) 1, true, (char) 2, (short) 3, 4, 5, 1f, 2d, "test"));

        BufferObjectDataInput in = service2.createObjectDataInput(data);
        PortableSerializer portableSerializer = service2.getPortableSerializer();
        reader = portableSerializer.createMorphingReader(in);
    }

    @After
    public void after() throws Exception {
        service1.dispose();
        service2.dispose();
    }

    @Test
    public void testReadInt() throws Exception {
        int aByte = reader.readInt("byte");
        int aShort = reader.readInt("short");
        int aChar = reader.readInt("char");
        int aInt = reader.readInt("int");

        assertEquals(1, aByte);
        assertEquals(3, aShort);
        assertEquals(2, aChar);
        assertEquals(4, aInt);
        assertEquals(0, reader.readInt("NO SUCH FIELD"));

    }

    @Test
    public void testReadLong() throws Exception {
        long aByte = reader.readLong("byte");
        long aShort = reader.readLong("short");
        long aChar = reader.readLong("char");
        long aInt = reader.readLong("int");
        long aLong = reader.readLong("long");

        assertEquals(1, aByte);
        assertEquals(3, aShort);
        assertEquals(2, aChar);
        assertEquals(4, aInt);
        assertEquals(5, aLong);
        assertEquals(0, reader.readLong("NO SUCH FIELD"));

    }

    @Test
    public void testReadUTF() throws Exception {
        String aString = reader.readString("string");

        assertEquals("test", aString);
        assertNull(reader.readString("NO SUCH FIELD"));
    }

    @Test
    public void testReadBoolean() throws Exception {
        boolean aBoolean = reader.readBoolean("boolean");

        assertTrue(aBoolean);
        assertFalse(reader.readBoolean("NO SUCH FIELD"));
    }

    @Test
    public void testReadByte() throws Exception {
        byte aByte = reader.readByte("byte");

        assertEquals(1, aByte);
        assertEquals(0, reader.readByte("NO SUCH FIELD"));
    }

    @Test
    public void testReadChar() throws Exception {
        char aChar = reader.readChar("char");

        assertEquals(2, aChar);
        assertEquals(0, reader.readChar("NO SUCH FIELD"));
    }

    @Test
    public void testReadDouble() throws Exception {
        double aByte = reader.readDouble("byte");
        double aShort = reader.readDouble("short");
        double aChar = reader.readDouble("char");
        double aInt = reader.readDouble("int");
        double aFloat = reader.readDouble("float");
        double aLong = reader.readDouble("long");
        double aDouble = reader.readDouble("double");

        assertEquals(1, aByte, 0);
        assertEquals(3, aShort, 0);
        assertEquals(2, aChar, 0);
        assertEquals(4, aInt, 0);
        assertEquals(5, aLong, 0);
        assertEquals(1f, aFloat, 0);
        assertEquals(2d, aDouble, 0);
        assertEquals(0, reader.readDouble("NO SUCH FIELD"), 0);
    }

    @Test
    public void testReadFloat() throws Exception {
        float aByte = reader.readFloat("byte");
        float aShort = reader.readFloat("short");
        float aChar = reader.readFloat("char");
        float aInt = reader.readFloat("int");
        float aFloat = reader.readFloat("float");

        assertEquals(1, aByte, 0);
        assertEquals(3, aShort, 0);
        assertEquals(2, aChar, 0);
        assertEquals(4, aInt, 0);
        assertEquals(1f, aFloat, 0);
        assertEquals(0, reader.readFloat("NO SUCH FIELD"), 0);
    }

    @Test
    public void testReadShort() throws Exception {
        int aByte = reader.readShort("byte");
        int aShort = reader.readShort("short");

        assertEquals(1, aByte);
        assertEquals(3, aShort);
        assertEquals(0, reader.readShort("NO SUCH FIELD"));
    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadInt_IncompatibleClass() throws Exception {
        reader.readInt("string");

    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadLong_IncompatibleClass() throws Exception {
        reader.readLong("string");
    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadUTF_IncompatibleClass() throws Exception {
        reader.readString("byte");
    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadBoolean_IncompatibleClass() throws Exception {
        reader.readBoolean("string");
    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadByte_IncompatibleClass() throws Exception {
        reader.readByte("string");
    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadChar_IncompatibleClass() throws Exception {
        reader.readChar("string");
    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadDouble_IncompatibleClass() throws Exception {
        reader.readDouble("string");
    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadFloat_IncompatibleClass() throws Exception {
        reader.readFloat("string");
    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadShort_IncompatibleClass() throws Exception {
        reader.readShort("string");
    }

    @Test
    public void testReadByteArray() throws Exception {
        assertNull(reader.readByteArray("NO SUCH FIELD"));
    }

    @Test
    public void testReadCharArray() throws Exception {
        assertNull(reader.readCharArray("NO SUCH FIELD"));
    }

    @Test
    public void testReadIntArray() throws Exception {
        assertNull(reader.readIntArray("NO SUCH FIELD"));
    }

    @Test
    public void testReadLongArray() throws Exception {
        assertNull(reader.readLongArray("NO SUCH FIELD"));
    }

    @Test
    public void testReadDoubleArray() throws Exception {
        assertNull(reader.readDoubleArray("NO SUCH FIELD"));
    }

    @Test
    public void testReadFloatArray() throws Exception {
        assertNull(reader.readFloatArray("NO SUCH FIELD"));
    }

    @Test
    public void testReadShortArray() throws Exception {
        assertNull(reader.readShortArray("NO SUCH FIELD"));
    }

    @Test
    public void testReadUTFArray() throws Exception {
        assertNull(reader.readStringArray("NO SUCH FIELD"));
    }

    @Test
    public void testReadPortable() throws Exception {
        assertNull(reader.readPortable("NO SUCH FIELD"));
    }

    @Test
    public void testReadPortableArray() throws Exception {
        assertNull(reader.readPortableArray("NO SUCH FIELD"));
    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadByteArray_IncompatibleClass() throws Exception {
        reader.readByteArray("byte");
    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadCharArray_IncompatibleClass() throws Exception {
        reader.readCharArray("byte");
    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadIntArray_IncompatibleClass() throws Exception {
        reader.readIntArray("byte");
    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadLongArray_IncompatibleClass() throws Exception {
        reader.readByteArray("byte");
    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadDoubleArray_IncompatibleClass() throws Exception {
        reader.readByteArray("byte");
    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadFloatArray_IncompatibleClass() throws Exception {
        reader.readByteArray("byte");
    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadShortArray_IncompatibleClass() throws Exception {
        reader.readByteArray("byte");
    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadPortable_IncompatibleClass() throws Exception {
        reader.readByteArray("byte");
    }

    @Test(expected = IncompatibleClassChangeError.class)
    public void testReadPortableArray_IncompatibleClass() throws Exception {
        reader.readByteArray("byte");
    }

}
