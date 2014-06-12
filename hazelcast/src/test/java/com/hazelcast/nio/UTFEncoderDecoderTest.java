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
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Random;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class UTFEncoderDecoderTest extends HazelcastTestSupport {

    private static final Random RANDOM = new Random();
    private static final int BENCHMARK_ROUNDS = 10; // 100;

    @Test
    public void testEmptyText_Default() throws Exception {
        byte[] buffer = new byte[1024];

        UTFEncoderDecoder utfEncoderDecoder = newUTFEncoderDecoder(false);
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
    public void testEmptyText_Fast() throws Exception {
        byte[] buffer = new byte[1024];

        UTFEncoderDecoder utfEncoderDecoder = newUTFEncoderDecoder(true);
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
        byte[] buffer = new byte[1024];

        UTFEncoderDecoder utfEncoderDecoder = newUTFEncoderDecoder(false);

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
    public void testMultipleTextsInARow_fast() throws Exception {
        byte[] buffer = new byte[1024];

        UTFEncoderDecoder utfEncoderDecoder = newUTFEncoderDecoder(true);

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

// TODO: This test does assume TimedMemberState has UTF strings inside,
// since TimedMemberState is not DataSerializable anymore,
// this test needs to be rewritten with a dummy complex object.
//    @Test
//    public void testComplexObject() throws Exception {
//        HazelcastInstance hz = createHazelcastInstance();
//        Field original = HazelcastInstanceProxy.class.getDeclaredField("original");
//        original.setAccessible(true);
//
//        HazelcastInstanceImpl impl = (HazelcastInstanceImpl) original.get(hz);
//        TimedMemberStateFactory timedMemberStateFactory = new TimedMemberStateFactory(impl);
//        TimedMemberState memberState = timedMemberStateFactory.createTimedMemberState();
//
//        SerializationService ss = impl.node.getSerializationService();
//
//        ByteArrayOutputStream baos = new ByteArrayOutputStream(2048);
//        ObjectDataOutput out = ss.createObjectDataOutputStream(baos);
//        out.writeObject(memberState);
//
//        ObjectDataInput in = ss.createObjectDataInput(baos.toByteArray());
//        TimedMemberState result = in.readObject();
//
//        assertEquals(memberState, result);
//    }

    @Test
    public void testShortSizedText_1Chunk_Default() throws Exception {
        byte[] buffer = new byte[1024];

        UTFEncoderDecoder utfEncoderDecoder = newUTFEncoderDecoder(false);
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
    public void testShortSizedText_1Chunk_Fast() throws Exception {
        byte[] buffer = new byte[1024];

        UTFEncoderDecoder utfEncoderDecoder = newUTFEncoderDecoder(true);
        assertContains(utfEncoderDecoder.getStringCreator().getClass().toString(), "FastStringCreator");

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
        byte[] buffer = new byte[1024];

        UTFEncoderDecoder utfEncoderDecoder = newUTFEncoderDecoder(false);
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
    public void testMiddleSizedText_2Chunks_Fast() throws Exception {
        byte[] buffer = new byte[1024];

        UTFEncoderDecoder utfEncoderDecoder = newUTFEncoderDecoder(true);
        assertContains(utfEncoderDecoder.getStringCreator().getClass().toString(), "FastStringCreator");

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
        byte[] buffer = new byte[1024];

        UTFEncoderDecoder utfEncoderDecoder = newUTFEncoderDecoder(false);
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
    public void testLongSizedText_min3Chunks_Fast() throws Exception {
        byte[] buffer = new byte[1024];

        UTFEncoderDecoder utfEncoderDecoder = newUTFEncoderDecoder(true);
        assertContains(utfEncoderDecoder.getStringCreator().getClass().toString(), "FastStringCreator");

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

    private static void assertContains(String className, String classType) {
        if (className.contains(classType)) {
            return;
        }
        throw new AssertionError(className + " does not contains " + classType);
    }

    private static String random(int count) {
        return random(count, 0, 0, true, true, null, RANDOM);
    }

    private static UTFEncoderDecoder newUTFEncoderDecoder(boolean fastStringCreator) {
        try {
            Constructor<UTFEncoderDecoder> constructor = UTFEncoderDecoder.class.getDeclaredConstructor(boolean.class);
            constructor.setAccessible(true);
            return constructor.newInstance(fastStringCreator);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

}
