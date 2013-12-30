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

import com.hazelcast.nio.utf8.StringCreatorUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.JvmUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Method;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class UTFUtilTest {

    private static final Random RANDOM = new Random(-System.nanoTime());
    private static final int BENCHMARK_ROUNDS = 10; // 100;

    private static final boolean[][] PARAMETERS = {
            // [0] = avail outside Sun / Oracle JVM
            // [1] = use faststring
            // [2] = enable Java8 way (not hooked that deep into the JVM)
            // [3] = enable ASM generator
            // [4] = enable BCEL generator
            // [5] = enable JVM internal BCEL generator (com.sun.org.apache.bcel.internal)
            // [6] = enable Javassist generator
            {true, false, false, false, false, false, false},
            {true, true, false, false, false, false, false},
            {false, true, false, true, false, false, false},
            {false, true, false, false, true, false, false},
            {false, true, false, false, false, true, false},
            {false, true, false, false, false, false, true},
            {false, true, true, true, false, false, false},
            {false, true, true, false, true, false, false},
            {false, true, true, false, false, true, false},
            {false, true, true, false, false, false, true}
    };

    private static final String TYPE_DEFAULT = "DefaultStringCreator";
    private static final String TYPE_FASTSTRING = "FastStringCreator";
    private static final String TYPE_JAVA8_ASM = "AsmStringAccessor";
    private static final String TYPE_JAVA8_BCEL = "BcelStringAccessor";
    private static final String TYPE_JAVA8_INTERNAL_BCEL = "InternalBcelStringAccessor";
    private static final String TYPE_JAVA8_JAVASSIST = "JavassistStringAccessor";
    private static final String TYPE_MAGIC_ASM = "AsmMagicAccessorStringCreatorBuilder$2";
    private static final String TYPE_MAGIC_BCEL = "BcelMagicAccessorStringCreatorBuilder$2";
    private static final String TYPE_MAGIC_ORACLE_INTERNAL_BCEL = "OracleBcelMagicAccessorStringCreatorBuilder$2";
    private static final String TYPE_MAGIC_IBM_INTERNAL_BCEL = "IBMBcelMagicAccessorStringCreatorBuilder$2";
    private static final String TYPE_MAGIC_JAVASSIST = "JavassistMagicAccessorStringCreatorBuilder$2";

    private static final String[] CLASSTYPES_ORACLE_JAVA8 = {
            TYPE_DEFAULT, TYPE_FASTSTRING, TYPE_MAGIC_ASM, TYPE_MAGIC_BCEL, TYPE_MAGIC_ORACLE_INTERNAL_BCEL,
            TYPE_MAGIC_JAVASSIST, TYPE_JAVA8_ASM, TYPE_JAVA8_BCEL, TYPE_JAVA8_INTERNAL_BCEL,
            TYPE_JAVA8_JAVASSIST
    };

    private static final String[] CLASSTYPES_IBM_JAVA8 = {
            TYPE_DEFAULT, TYPE_FASTSTRING, TYPE_MAGIC_ASM, TYPE_MAGIC_BCEL, TYPE_MAGIC_IBM_INTERNAL_BCEL,
            TYPE_MAGIC_JAVASSIST, TYPE_JAVA8_ASM, TYPE_JAVA8_BCEL, TYPE_JAVA8_INTERNAL_BCEL,
            TYPE_JAVA8_JAVASSIST
    };

    private static final String[] CLASSTYPES_ORACLE_JAVA6 = {
            TYPE_DEFAULT, TYPE_FASTSTRING, TYPE_MAGIC_ASM, TYPE_MAGIC_BCEL,
            TYPE_MAGIC_ORACLE_INTERNAL_BCEL, TYPE_MAGIC_JAVASSIST
    };

    private static final String[] CLASSTYPES_IBM_JAVA6 = {
            TYPE_DEFAULT, TYPE_FASTSTRING, TYPE_MAGIC_ASM, TYPE_MAGIC_BCEL,
            TYPE_MAGIC_IBM_INTERNAL_BCEL, TYPE_MAGIC_JAVASSIST
    };

    private static final String[] CLASSTYPES;

    static {
        if (JvmUtil.getJvmVendor() == JvmUtil.Vendor.IBM) {
            if (JvmUtil.getJvmVersion() == JvmUtil.Version.Java8) {
                CLASSTYPES = CLASSTYPES_IBM_JAVA8;
            } else {
                CLASSTYPES = CLASSTYPES_IBM_JAVA6;
            }
        } else {
            if (isOracleJava8()) {
                CLASSTYPES = CLASSTYPES_ORACLE_JAVA8;
            } else {
                CLASSTYPES = CLASSTYPES_ORACLE_JAVA6;
            }
        }
    }

    @Test
    public void testShortSizedText_1Chunk() throws Exception {
        byte[] buffer = new byte[1024];
        for (int z = 0; z < PARAMETERS.length; z++) {
            boolean[] parameters = PARAMETERS[z];
            if (!executeTest(parameters)) {
                continue;
            }

            boolean faststringEnabled = parameters[1];
            boolean java8Enabled = parameters[2];
            boolean asmEnabled = parameters[3];
            boolean bcelEnabled = parameters[4];
            boolean internalBcelEnabled = parameters[5];
            boolean javassistEnabled = parameters[6];

            UTFUtil.StringCreator stringCreator = StringCreatorUtil.findBestStringCreator(faststringEnabled,
                    java8Enabled, asmEnabled, bcelEnabled, internalBcelEnabled, javassistEnabled, false);

            String className = stringCreator.getClass().toString();
            assertContains(className, CLASSTYPES[z]);

            UTFUtil utfUtil = new UTFUtil(stringCreator);

            for (int o = 0; o < BENCHMARK_ROUNDS; o++) {
                for (int i = 2; i < 100; i += 2) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream(500);
                    DataOutputStream dos = new DataOutputStream(baos);

                    String randomString = random(i * 100);
                    utfUtil.writeUTF(dos, randomString, buffer);

                    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
                    DataInputStream dis = new DataInputStream(bais);
                    String result = utfUtil.readUTF(dis, buffer);

                    assertEquals(randomString, result);
                }
            }
        }
    }

    @Test
    public void testMiddleSizedText_2Chunks() throws Exception {
        byte[] buffer = new byte[1024];
        for (int z = 0; z < PARAMETERS.length; z++) {
            boolean[] parameters = PARAMETERS[z];
            if (!executeTest(parameters)) {
                continue;
            }

            boolean faststringEnabled = parameters[1];
            boolean java8Enabled = parameters[2];
            boolean asmEnabled = parameters[3];
            boolean bcelEnabled = parameters[4];
            boolean internalBcelEnabled = parameters[5];
            boolean javassistEnabled = parameters[6];

            UTFUtil.StringCreator stringCreator = StringCreatorUtil.findBestStringCreator(faststringEnabled,
                    java8Enabled, asmEnabled, bcelEnabled, internalBcelEnabled, javassistEnabled, false);

            String className = stringCreator.getClass().toString();
            assertContains(className, CLASSTYPES[z]);

            UTFUtil utfUtil = new UTFUtil(stringCreator);

            for (int o = 0; o < BENCHMARK_ROUNDS; o++) {
                for (int i = 170; i < 300; i += 2) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream(500);
                    DataOutputStream dos = new DataOutputStream(baos);

                    String randomString = random(i * 100);
                    utfUtil.writeUTF(dos, randomString, buffer);

                    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
                    DataInputStream dis = new DataInputStream(bais);
                    String result = utfUtil.readUTF(dis, buffer);

                    assertEquals(randomString, result);
                }
            }
        }
    }

    @Test
    public void testLongSizedText_min3Chunks() throws Exception {
        byte[] buffer = new byte[1024];
        for (int z = 0; z < PARAMETERS.length; z++) {
            boolean[] parameters = PARAMETERS[z];
            if (!executeTest(parameters)) {
                continue;
            }

            boolean faststringEnabled = parameters[1];
            boolean java8Enabled = parameters[2];
            boolean asmEnabled = parameters[3];
            boolean bcelEnabled = parameters[4];
            boolean internalBcelEnabled = parameters[5];
            boolean javassistEnabled = parameters[6];

            System.out.println("Running test for " + CLASSTYPES[z]);

            UTFUtil.StringCreator stringCreator = StringCreatorUtil.findBestStringCreator(faststringEnabled,
                    java8Enabled, asmEnabled, bcelEnabled, internalBcelEnabled, javassistEnabled, false);

            String className = stringCreator.getClass().toString();
            assertContains(className, CLASSTYPES[z]);

            UTFUtil utfUtil = new UTFUtil(stringCreator);

            for (int o = 0; o < BENCHMARK_ROUNDS; o++) {
                for (int i = 330; i < 900; i += 5) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream(500);
                    DataOutputStream dos = new DataOutputStream(baos);

                    String randomString = random(i * 100);
                    utfUtil.writeUTF(dos, randomString, buffer);

                    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
                    DataInputStream dis = new DataInputStream(bais);
                    String result = utfUtil.readUTF(dis, buffer);

                    assertEquals(randomString, result);
                }
            }
        }
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

    private static boolean isOracleJava8() {
        try {
            Class<?> clazz = Class.forName("sun.misc.JavaLangAccess");
            Method method = clazz.getDeclaredMethod("newStringUnsafe", char[].class);
            if (method.getReturnType().equals(String.class)) {
                return true;
            }
        } catch (Throwable ignore) {
        }
        return false;
    }

    private static boolean executeTest(boolean[] parameters) {
        if (JvmUtil.getJvmVendor() == JvmUtil.Vendor.SunOracle) {
            if (isOracleJava8()) {
                return true;
            }
            return !parameters[2];
        }
        if (JvmUtil.getJvmVendor() == JvmUtil.Vendor.IBM) {
            return !parameters[2];
        }
        for (int i = 2; i < parameters.length; i++) {
            if (parameters[i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean isMagicAccessorJVM() {
        try {
            Class.forName("sun.reflect.MagicAccessorImpl");
            return true;
        } catch (Throwable ignore) {
        }
        return false;
    }

}
