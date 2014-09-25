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

package com.hazelcast.nio;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UTFDataFormatException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static com.hazelcast.nio.UTFEncoderDecoder.ReflectionBasedCharArrayUtfWriter;
import static com.hazelcast.nio.UTFEncoderDecoder.UnsafeBasedCharArrayUtfWriter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class UTFEncoderDecoderTest extends HazelcastTestSupport {

    private static final Random RANDOM = new Random();
    private static final int BENCHMARK_ROUNDS = 10; // 100;

    @Test(expected = IllegalArgumentException.class)
    public void testReadUTF_bufferSizeMustAlwaysBePowerOfTwo() throws IOException {
        byte[] buffer = new byte[1023];

        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
        DataInputStream dis = new DataInputStream(bais);
        UTFEncoderDecoder.readUTF(dis, buffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteUTF_bufferSizeMustAlwaysBePowerOfTwo() throws IOException {
        byte[] buffer = new byte[1023];

        DataOutput dataOutput = new DataOutputStream(new ByteArrayOutputStream());
        UTFEncoderDecoder.writeUTF(dataOutput, "foo", buffer);
    }

    @Test
    public void testEmptyText_Default() throws Exception {
       testEmptyText(false, UtfWriterType.DEFAULT);
    }

    @Test
    public void testEmptyText_Unsafe() throws Exception {
        testEmptyText(false, UtfWriterType.UNSAFE);
    }

    @Test
    public void testEmptyText_Reflection() throws Exception {
        testEmptyText(false, UtfWriterType.REFLECTION);
    }

    @Test
    public void testEmptyText_Fast_Default() throws Exception {
        testEmptyText(true, UtfWriterType.DEFAULT);
    }

    @Test
    public void testEmptyText_Fast_Unsafe() throws Exception {
        testEmptyText(true, UtfWriterType.UNSAFE);
    }

    @Test
    public void testEmptyText_Fast_Reflection() throws Exception {
        testEmptyText(true, UtfWriterType.REFLECTION);
    }

    private void testEmptyText(boolean fastStringEnabled, UtfWriterType utfWriterType) throws Exception {
        UTFEncoderDecoder utfEncoderDecoder = newUTFEncoderDecoder(fastStringEnabled, utfWriterType);
        if (utfEncoderDecoder == null) {
            System.err.println("Ignoring test... " + utfWriterType + " is not available!");
            return;
        }

        byte[] buffer = new byte[1024];
        ByteArrayOutputStream baos = new ByteArrayOutputStream(500);
        DataOutputStream dos = new DataOutputStream(baos);

        utfEncoderDecoder.writeUTF0(dos, "", buffer);
        utfEncoderDecoder.writeUTF0(dos, "some other value", buffer);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        String result1 = utfEncoderDecoder.readUTF0(dis, buffer);
        String result2 = utfEncoderDecoder.readUTF0(dis, buffer);

        assertEquals("", result1);
        assertEquals("some other value", result2);
    }

    @Test
    public void testMultipleTextsInARow_Default() throws Exception {
        testMultipleTextsInARow(false, UtfWriterType.DEFAULT);
    }

    @Test
    public void testMultipleTextsInARow_Unsafe() throws Exception {
        testMultipleTextsInARow(false, UtfWriterType.UNSAFE);
    }

    @Test
    public void testMultipleTextsInARow_Reflection() throws Exception {
        testMultipleTextsInARow(false, UtfWriterType.REFLECTION);
    }

    @Test
    public void testMultipleTextsInARow_Fast_Default() throws Exception {
       testMultipleTextsInARow(true, UtfWriterType.DEFAULT);
    }

    @Test
    public void testMultipleTextsInARow_Fast_Unsafe() throws Exception {
        testMultipleTextsInARow(true, UtfWriterType.UNSAFE);
    }

    @Test
    public void testMultipleTextsInARow_Fast_Reflection() throws Exception {
        testMultipleTextsInARow(true, UtfWriterType.REFLECTION);
    }

    private void testMultipleTextsInARow(boolean fastStringEnabled, UtfWriterType utfWriterType) throws Exception {
        UTFEncoderDecoder utfEncoderDecoder = newUTFEncoderDecoder(fastStringEnabled, utfWriterType);
        if (utfEncoderDecoder == null) {
            System.err.println("Ignoring test... " + utfWriterType + " is not available!");
            return;
        }

        byte[] buffer = new byte[1024];
        for (int i = 0; i < 100; i++) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(500);
            DataOutputStream dos = new DataOutputStream(baos);

            String[] values = new String[10];
            for (int o = 0; o < 10; o++) {
                values[o] = random(i);
                utfEncoderDecoder.writeUTF0(dos, values[o], buffer);
            }

            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            DataInputStream dis = new DataInputStream(bais);

            for (int o = 0; o < 10; o++) {
                String result = utfEncoderDecoder.readUTF0(dis, buffer);
                assertEquals(values[o], result);
            }
        }
    }

    @Test
    public void testComplexObject() throws Exception {
        HazelcastInstance hz = createHazelcastInstance();
        Field original = HazelcastInstanceProxy.class.getDeclaredField("original");
        original.setAccessible(true);

        HazelcastInstanceImpl impl = (HazelcastInstanceImpl) original.get(hz);
        ComplexUtf8Object complexUtf8Object = buildComplexUtf8Object();

        SerializationService ss = impl.node.getSerializationService();

        ByteArrayOutputStream baos = new ByteArrayOutputStream(2048);
        ObjectDataOutput out = ss.createObjectDataOutputStream(baos);
        out.writeObject(complexUtf8Object);

        ObjectDataInput in = ss.createObjectDataInput(baos.toByteArray());
        ComplexUtf8Object result = in.readObject();

        assertEquals(complexUtf8Object, result);
    }

    @Test
    public void testShortSizedText_1Chunk_Default() throws Exception {
        testShortSizedText_1Chunk(false, UtfWriterType.DEFAULT);
    }

    @Test
    public void testShortSizedText_1Chunk_Unsafe() throws Exception {
        testShortSizedText_1Chunk(false, UtfWriterType.UNSAFE);
    }

    @Test
    public void testShortSizedText_1Chunk_Reflection() throws Exception {
        testShortSizedText_1Chunk(false, UtfWriterType.REFLECTION);
    }

    @Test
    public void testShortSizedText_1Chunk_Fast_Default() throws Exception {
        testShortSizedText_1Chunk(true, UtfWriterType.DEFAULT);
    }

    @Test
    public void testShortSizedText_1Chunk_Fast_Unsafe() throws Exception {
        testShortSizedText_1Chunk(true, UtfWriterType.UNSAFE);
    }

    @Test
    public void testShortSizedText_1Chunk_Fast_Reflection() throws Exception {
        testShortSizedText_1Chunk(true, UtfWriterType.REFLECTION);
    }

    private void testShortSizedText_1Chunk(boolean fastStringEnabled, UtfWriterType utfWriterType) throws Exception {
        UTFEncoderDecoder utfEncoderDecoder = newUTFEncoderDecoder(fastStringEnabled, utfWriterType);
        if (utfEncoderDecoder == null) {
            System.err.println("Ignoring test... " + utfWriterType + " is not available!");
            return;
        }

        byte[] buffer = new byte[1024];
        for (int o = 0; o < BENCHMARK_ROUNDS; o++) {
            for (int i = 2; i < 100; i += 2) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream(500);
                DataOutputStream dos = new DataOutputStream(baos);

                String randomString = random(i * 100);
                utfEncoderDecoder.writeUTF0(dos, randomString, buffer);

                ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
                DataInputStream dis = new DataInputStream(bais);
                String result = utfEncoderDecoder.readUTF0(dis, buffer);

                assertEquals(randomString, result);
            }
        }
    }

    @Test
    public void testMiddleSizedText_2Chunks_Default() throws Exception {
        testMiddleSizedText_2Chunks(false, UtfWriterType.DEFAULT);
    }

    @Test
    public void testMiddleSizedText_2Chunks_Unsafe() throws Exception {
        testMiddleSizedText_2Chunks(false, UtfWriterType.UNSAFE);
    }

    @Test
    public void testMiddleSizedText_2Chunks_Reflection() throws Exception {
        testMiddleSizedText_2Chunks(false, UtfWriterType.REFLECTION);
    }

    @Test
    public void testMiddleSizedText_2Chunks_Fast_Default() throws Exception {
        testMiddleSizedText_2Chunks(true, UtfWriterType.DEFAULT);
    }

    @Test
    public void testMiddleSizedText_2Chunks_Fast_Unsafe() throws Exception {
        testMiddleSizedText_2Chunks(true, UtfWriterType.UNSAFE);
    }

    @Test
    public void testMiddleSizedText_2Chunks_Fast_Reflection() throws Exception {
        testMiddleSizedText_2Chunks(true, UtfWriterType.REFLECTION);
    }

    private void testMiddleSizedText_2Chunks(boolean fastStringEnabled, UtfWriterType utfWriterType) throws Exception {
        UTFEncoderDecoder utfEncoderDecoder = newUTFEncoderDecoder(fastStringEnabled, utfWriterType);
        if (utfEncoderDecoder == null) {
            System.err.println("Ignoring test... " + utfWriterType + " is not available!");
            return;
        }

        byte[] buffer = new byte[1024];
        for (int o = 0; o < BENCHMARK_ROUNDS; o++) {
            for (int i = 170; i < 300; i += 2) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream(500);
                DataOutputStream dos = new DataOutputStream(baos);

                String randomString = random(i * 100);
                utfEncoderDecoder.writeUTF0(dos, randomString, buffer);

                ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
                DataInputStream dis = new DataInputStream(bais);
                String result = utfEncoderDecoder.readUTF0(dis, buffer);

                assertEquals(randomString, result);
            }
        }
    }

    @Test
    public void testLongSizedText_min3Chunks_Default() throws Exception {
        testLongSizedText_min3Chunks(false, UtfWriterType.DEFAULT);
    }

    @Test
    public void testLongSizedText_min3Chunks_Unsafe() throws Exception {
        testLongSizedText_min3Chunks(false, UtfWriterType.UNSAFE);
    }

    @Test
    public void testLongSizedText_min3Chunks_Reflection() throws Exception {
        testLongSizedText_min3Chunks(false, UtfWriterType.REFLECTION);
    }

    @Test
    public void testLongSizedText_min3Chunks_Fast_Default() throws Exception {
        testLongSizedText_min3Chunks(true, UtfWriterType.DEFAULT);
    }

    @Test
    public void testLongSizedText_min3Chunks_Fast_Unsafe() throws Exception {
        testLongSizedText_min3Chunks(true, UtfWriterType.UNSAFE);
    }

    @Test
    public void testLongSizedText_min3Chunks_Fast_Reflection() throws Exception {
        testLongSizedText_min3Chunks(true, UtfWriterType.REFLECTION);
    }

    private void testLongSizedText_min3Chunks(boolean fastStringEnabled, UtfWriterType utfWriterType) throws Exception {
        UTFEncoderDecoder utfEncoderDecoder = newUTFEncoderDecoder(fastStringEnabled, utfWriterType);
        if (utfEncoderDecoder == null) {
            System.err.println("Ignoring test... " + utfWriterType + " is not available!");
            return;
        }

        byte[] buffer = new byte[1024];
        for (int o = 0; o < BENCHMARK_ROUNDS; o++) {
            for (int i = 330; i < 900; i += 5) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream(500);
                DataOutputStream dos = new DataOutputStream(baos);

                String randomString = random(i * 100);
                utfEncoderDecoder.writeUTF0(dos, randomString, buffer);

                ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
                DataInputStream dis = new DataInputStream(bais);
                String result = utfEncoderDecoder.readUTF0(dis, buffer);

                assertEquals(randomString, result);
            }
        }
    }

    @Test
    public void testNullSerialization() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();

        IMap<String, User> map = instance1.getMap("testSerialization");
        map.put("1", new User("", "demirci"));

        User user = map.get("1");
        assertEquals("", user.getName());
        assertEquals("demirci", user.getSurname());
    }

    @Test
    public void testIssue2674_multibyte_char_at_position_that_even_multiple_of_buffer_size() throws Exception {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        for (int i : new int[]{50240, 100240, 80240}) {
            String originalString = createString(i);
            BufferObjectDataOutput dataOutput = serializationService.createObjectDataOutput(100000);
            dataOutput.writeUTF(originalString);
            BufferObjectDataInput dataInput = serializationService.createObjectDataInput(dataOutput.toByteArray());

            assertEquals(originalString, dataInput.readUTF());
        }
    }

    @Test
    public void testIssue2705_integer_overflow_on_old_version_multicast_package() throws Exception {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream(64000);
        byte[] temp = new byte[1024];
        InputStream is = UTFEncoderDecoderTest.class.getResourceAsStream("hz-3.1.5-multicast-package.dump");
        int length;
        while ((length = is.read(temp)) != -1) {
            baos.write(temp, 0, length);
        }

        BufferObjectDataInput dataInput = serializationService.createObjectDataInput(baos.toByteArray());
        dataInput.position(1);

        try {
            dataInput.readObject();
        } catch (HazelcastSerializationException e) {
            if (e.getCause() == null || !(e.getCause() instanceof UTFDataFormatException)) {
                fail("Expected UTFDataFormatException");
            }
            return;
        }
        fail("HazelcastSerializationException is expected");
    }

    @Test
    public void testSubstring_Default() throws IOException {
        testSubstring(false, UtfWriterType.DEFAULT);
    }

    @Test
    public void testSubstring_Unsafe() throws IOException {
        testSubstring(false, UtfWriterType.UNSAFE);
    }

    @Test
    public void testSubstring_Reflection() throws IOException {
        testSubstring(false, UtfWriterType.REFLECTION);
    }

    @Test
    public void testSubstring_Fast_Default() throws IOException {
        testSubstring(true, UtfWriterType.DEFAULT);
    }

    @Test
    public void testSubstring_Fast_Unsafe() throws IOException {
        testSubstring(true, UtfWriterType.UNSAFE);
    }

    @Test
    public void testSubstring_Fast_Reflection() throws IOException {
        testSubstring(true, UtfWriterType.REFLECTION);
    }

    private void testSubstring(boolean fastStringEnabled, UtfWriterType utfWriterType) throws IOException {
        UTFEncoderDecoder utfEncoderDecoder = newUTFEncoderDecoder(fastStringEnabled, utfWriterType);
        if (utfEncoderDecoder == null) {
            System.err.println("Ignoring test... " + utfWriterType + " is not available!");
            return;
        }

        byte[] buffer = new byte[64];
        String original = "1234abcd";
        String str = original.substring(4);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        utfEncoderDecoder.writeUTF0(dos, str, buffer);

        byte[] bytes = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bais);
        String result = utfEncoderDecoder.readUTF0(dis, buffer);

        assertEquals(str, result);
    }

    private String createString(int length) {
        char[] c = new char[length];
        for (int i = 0; i < c.length; i++) {
            c[i] = 'a';
        }
        c[10240] = 'å';
        return new String(c);
    }

    private static void assertContains(String className, String classType) {
        if (className.contains(classType)) {
            return;
        }
        throw new AssertionError(className + " does not contains " + classType);
    }

    private static String random(int count) {
        return random(count, 0, 0, true, true, null, RANDOM);
    }

    private static UTFEncoderDecoder newUTFEncoderDecoder(boolean fastStringEnabled, UtfWriterType utfWriterType) {
        UTFEncoderDecoder.UtfWriter utfWriter;
        switch (utfWriterType) {
            case UNSAFE:
                UnsafeBasedCharArrayUtfWriter unsafeBasedWriter = new UnsafeBasedCharArrayUtfWriter();
                if (!unsafeBasedWriter.isAvailable()) {
                    return null;
                }
                utfWriter = unsafeBasedWriter;
                break;

            case REFLECTION:
                ReflectionBasedCharArrayUtfWriter reflectionBasedWriter = new ReflectionBasedCharArrayUtfWriter();
                if (!reflectionBasedWriter.isAvailable()) {
                    return null;
                }
                utfWriter = reflectionBasedWriter;
                break;

            default:
                utfWriter = new UTFEncoderDecoder.StringBasedUtfWriter();
        }

        UTFEncoderDecoder.StringCreator stringCreator = UTFEncoderDecoder.createStringCreator(fastStringEnabled);
        if (fastStringEnabled) {
            assertContains(stringCreator.getClass().toString(), "FastStringCreator");
        }

        return new UTFEncoderDecoder(stringCreator, utfWriter);
    }

    private static ComplexUtf8Object buildComplexUtf8Object() {
        ComplexUtf8Object object = new ComplexUtf8Object();

        object.clusterName = random(20);
        object.instanceNames = new HashSet<String>();
        for (int i = 0; i < 20; i++) {
            object.instanceNames.add(randomString());
        }
        object.master = RANDOM.nextBoolean();
        object.memberList = new ArrayList<String>();
        for (int i = 0; i < 200; i++) {
            object.memberList.add(randomString());
        }
        object.time = RANDOM.nextLong();
        object.innerObject = new InnerComplexUtf8Object();
        object.innerObject.memberList = new ArrayList<String>();
        for (int i = 0; i < 200; i++) {
            object.innerObject.memberList.add(random(200));
        }

        return object;
    }

    /*
     * Thanks to Apache Commons:
     * org.apache.commons.lang.RandomStringUtils
     */
    private static String random(int count, int start, int end, boolean letters,
                                 boolean numbers, char[] chars, Random random) {
        if (count == 0) {
            return "";
        } else if (count < 0) {
            throw new IllegalArgumentException("Requested random string length " + count + " is less than 0.");
        }
        if ((start == 0) && (end == 0)) {
            end = 'z' + 1;
            start = ' ';
            if (!letters && !numbers) {
                start = 0;
                end = Integer.MAX_VALUE;
            }
        }

        char[] buffer = new char[count];
        int gap = end - start;

        while (count-- != 0) {
            char ch;
            if (chars == null) {
                ch = (char) (random.nextInt(gap) + start);
            } else {
                ch = chars[random.nextInt(gap) + start];
            }
            //if ((letters && Character.isLetter(ch))
            //        || (numbers && Character.isDigit(ch))
            //        || (!letters && !numbers)) {
            if (ch >= 56320 && ch <= 57343) {
                if (count == 0) {
                    count++;
                } else {
                    // low surrogate, insert high surrogate after putting it in
                    buffer[count] = ch;
                    count--;
                    buffer[count] = (char) (55296 + random.nextInt(128));
                }
            } else if (ch >= 55296 && ch <= 56191) {
                if (count == 0) {
                    count++;
                } else {
                    // high surrogate, insert low surrogate before putting it in
                    buffer[count] = (char) (56320 + random.nextInt(128));
                    count--;
                    buffer[count] = ch;
                }
            } else if (ch >= 56192 && ch <= 56319) {
                // private high surrogate, no effing clue, so skip it
                count++;
            } else {
                buffer[count] = ch;
            }
            //} else {
            //    count++;
            //}
        }
        return new String(buffer);
    }

    private enum UtfWriterType {
        UNSAFE,
        REFLECTION,
        DEFAULT
    }

    public static class User implements DataSerializable {

        private String name;
        private String surname;

        public User() {
        }

        public User(String name, String surname) {
            this.name = name;
            this.surname = surname;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSurname() {
            return surname;
        }

        public void setSurname(String surname) {
            this.surname = surname;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(name);
            out.writeUTF(surname);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            name = in.readUTF();
            surname = in.readUTF();
        }
    }

    public static class ComplexUtf8Object implements DataSerializable {
        private long time;
        private InnerComplexUtf8Object innerObject = null;
        private Set<String> instanceNames = null;
        private List<String> memberList;
        private Boolean master;
        private String clusterName;

        public void writeData(ObjectDataOutput out)
                throws IOException {
            out.writeLong(time);
            out.writeBoolean(master);
            innerObject.writeData(out);
            out.writeUTF(clusterName);
            int nameCount = (instanceNames == null) ? 0 : instanceNames.size();
            out.writeInt(nameCount);
            if (instanceNames != null) {
                for (String name : instanceNames) {
                    out.writeUTF(name);
                }
            }
            int memberCount = (memberList == null) ? 0 : memberList.size();
            out.writeInt(memberCount);
            if (memberList != null) {
                for (String address : memberList) {
                    out.writeUTF(address);
                }
            }
        }

        public void readData(ObjectDataInput in)
                throws IOException {
            time = in.readLong();
            master = in.readBoolean();
            innerObject = new InnerComplexUtf8Object();
            innerObject.readData(in);
            clusterName = in.readUTF();
            int nameCount = in.readInt();
            instanceNames = new HashSet<String>(nameCount);
            for (int i = 0; i < nameCount; i++) {
                instanceNames.add(in.readUTF());
            }
            int memberCount = in.readInt();
            memberList = new ArrayList<String>();
            for (int i = 0; i < memberCount; i++) {
                memberList.add(in.readUTF());
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ComplexUtf8Object that = (ComplexUtf8Object) o;

            if (time != that.time) {
                return false;
            }
            if (clusterName != null ? !clusterName.equals(that.clusterName) : that.clusterName != null) {
                return false;
            }
            if (innerObject != null ? !innerObject.equals(that.innerObject) : that.innerObject != null) {
                return false;
            }
            if (instanceNames != null ? !instanceNames.equals(that.instanceNames) : that.instanceNames != null) {
                return false;
            }
            if (master != null ? !master.equals(that.master) : that.master != null) {
                return false;
            }
            if (memberList != null ? !memberList.equals(that.memberList) : that.memberList != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (time ^ (time >>> 32));
            result = 31 * result + (innerObject != null ? innerObject.hashCode() : 0);
            result = 31 * result + (instanceNames != null ? instanceNames.hashCode() : 0);
            result = 31 * result + (memberList != null ? memberList.hashCode() : 0);
            result = 31 * result + (master != null ? master.hashCode() : 0);
            result = 31 * result + (clusterName != null ? clusterName.hashCode() : 0);
            return result;
        }
    }

    public static class InnerComplexUtf8Object implements DataSerializable {
        private List<String> memberList;

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {
            int memberCount = (memberList == null) ? 0 : memberList.size();
            out.writeInt(memberCount);
            if (memberList != null) {
                for (String address : memberList) {
                    out.writeUTF(address);
                }
            }
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {
            int memberCount = in.readInt();
            memberList = new ArrayList<String>();
            for (int i = 0; i < memberCount; i++) {
                memberList.add(in.readUTF());
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            InnerComplexUtf8Object that = (InnerComplexUtf8Object) o;

            if (memberList != null ? !memberList.equals(that.memberList) : that.memberList != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            return memberList != null ? memberList.hashCode() : 0;
        }
    }
}
