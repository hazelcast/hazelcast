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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class UTFUtilTest {

    private static final Random RANDOM = new Random(-System.nanoTime());
    private static final int BENCHMARK_ROUNDS = 1; // 100;

    @Test
    public void testShortSizedText_1Chunk() throws Exception {
        for (int o = 0; o < BENCHMARK_ROUNDS; o++) {
            for (int i = 2; i < 100; i += 2) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream(500);
                DataOutputStream dos = new DataOutputStream(baos);

                String randomString = random(i * 100);
                UTFUtil.writeUTF(dos, randomString);

                ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
                DataInputStream dis = new DataInputStream(bais);
                String result = UTFUtil.readUTF(dis);

                assertEquals(randomString, result);
            }
        }
    }

    @Test
    public void testMiddleSizedText_2Chunks() throws Exception {
        for (int o = 0; o < BENCHMARK_ROUNDS; o++) {
            for (int i = 170; i < 300; i += 2) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream(500);
                DataOutputStream dos = new DataOutputStream(baos);

                String randomString = random(i * 100);
                UTFUtil.writeUTF(dos, randomString);

                ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
                DataInputStream dis = new DataInputStream(bais);
                String result = UTFUtil.readUTF(dis);

                assertEquals(randomString, result);
            }
        }
    }

    @Test
    public void testLongSizedText_min3Chunks() throws Exception {
        for (int o = 0; o < BENCHMARK_ROUNDS; o++) {
            for (int i = 330; i < 900; i += 5) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream(500);
                DataOutputStream dos = new DataOutputStream(baos);

                String randomString = random(i * 100);
                UTFUtil.writeUTF(dos, randomString);

                ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
                DataInputStream dis = new DataInputStream(bais);
                String result = UTFUtil.readUTF(dis);

                assertEquals(randomString, result);
            }
        }
    }

    private static String random(int count) {
        return random(count, 0, 0, true, true, null, RANDOM);
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

}
