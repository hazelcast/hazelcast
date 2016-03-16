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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class StringSerializationTest {

    private InternalSerializationService serializationService;

    private final static String TEST_DATA_TURKISH = "Pijamalı hasta, yağız şoföre çabucak güvendi.";
    private final static String TEST_DATA_JAPANESE = "イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム";
    private final static String TEST_DATA_ASCII = "The quick brown fox jumps over the lazy dog";
    private final static String TEST_DATA_ALL = TEST_DATA_TURKISH + TEST_DATA_JAPANESE + TEST_DATA_ASCII;
    private final static int TEST_STR_SIZE = 1 << 20;

    private final static byte[] TEST_DATA_BYTES_ALL = TEST_DATA_ALL.getBytes(Charset.forName("utf8"));

    private static final char[] allChars;

    static {
        CharBuffer cb = CharBuffer.allocate(Character.MAX_VALUE);
        for (char c = 0; c < Character.MAX_VALUE; c++) {
            if (Character.isLetter(c)) {
                cb.append(c);
            }
        }
        allChars = cb.array();
    }

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @After
    public void tearDown() {
        serializationService.dispose();
    }

    @Test
    public void testStringEncode()
            throws IOException {
        byte[] expected = toDataByte(TEST_DATA_BYTES_ALL, TEST_DATA_ALL.length());
        byte[] actual = serializationService.toBytes(TEST_DATA_ALL);
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testStringDecode()
            throws IOException {
        Data data = new HeapData(toDataByte(TEST_DATA_BYTES_ALL, TEST_DATA_ALL.length()));
        String actualStr = serializationService.toObject(data);
        assertEquals(TEST_DATA_ALL, actualStr);
    }

    @Test
    public void testStringAllCharLetterEncode()
            throws IOException {
        String allstr = new String(allChars);
        byte[] expected = allstr.getBytes(Charset.forName("utf8"));
        byte[] bytes = serializationService.toBytes(allstr);
        byte[] actual = Arrays.copyOfRange(bytes, HeapData.DATA_OFFSET + Bits.INT_SIZE_IN_BYTES, bytes.length);
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testLargeStringEncodeDecode()
            throws IOException {
        StringBuilder sb = new StringBuilder();
        int i = 0, j = 0;
        while (j < TEST_STR_SIZE) {
            int ch = i++ % Character.MAX_VALUE;
            if (Character.isLetter(ch)) {
                sb.append(ch);
                j++;
            }
        }
        String actualStr = sb.toString();
        byte[] strBytes = actualStr.getBytes(Charset.forName("utf8"));
        byte[] actualDataBytes = serializationService.toBytes(actualStr);
        byte[] expectedDataByte = toDataByte(strBytes, actualStr.length());
        String decodedStr = serializationService.toObject(new HeapData(expectedDataByte));
        assertArrayEquals("Deserialized byte array do not match utf-8 encoding", expectedDataByte, actualDataBytes);
        assertEquals(decodedStr, actualStr);
    }

    @Test
    public void testNullStringEncodeDecode()
            throws IOException {
        Data nullData = serializationService.toData(null);
        String decodedStr = serializationService.toObject(nullData);

        assertNull(decodedStr);
    }

    @Test
    public void testNullStringEncodeDecode2()
            throws IOException {
        BufferObjectDataOutput objectDataOutput = serializationService.createObjectDataOutput();
        objectDataOutput.writeUTF(null);

        byte[] bytes = objectDataOutput.toByteArray();
        objectDataOutput.close();

        BufferObjectDataInput objectDataInput = serializationService.createObjectDataInput(bytes);

        String decodedStr = objectDataInput.readUTF();
        assertNull(decodedStr);
    }

    @Test
    public void testStringAllCharLetterDecode()
            throws IOException {
        String allstr = new String(allChars);
        byte[] expected = allstr.getBytes(Charset.forName("utf8"));
        Data data = new HeapData(toDataByte(expected, allstr.length()));
        String actualStr = serializationService.toObject(data);
        assertEquals(allstr, actualStr);
    }

    @Test
    public void testStringArrayEncodeDecode()
            throws IOException {
        String[] stringArray = new String[100];
        for (int i = 0; i < stringArray.length; i++) {
            stringArray[i] = TEST_DATA_ALL + i;
        }
        Data dataStrArray = serializationService.toData(stringArray);
        String[] actualStr = serializationService.toObject(dataStrArray);

        assertEquals(SerializationConstants.CONSTANT_TYPE_STRING_ARRAY, dataStrArray.getType());
        assertArrayEquals(stringArray, actualStr);
    }

    private byte[] toDataByte(byte[] input, int length) {
        //the first 4 byte of type id, 4 byte string length and last 4 byte of partition hashCode
        ByteBuffer bf = ByteBuffer.allocate(input.length + 12);
        bf.putInt(0);
        bf.putInt(SerializationConstants.CONSTANT_TYPE_STRING);
        bf.putInt(length);
        bf.put(input);
        return bf.array();
    }
}
