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

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class StringSerializationTest {

    private static final String TEST_DATA_TURKISH = "Pijamalı hasta, yağız şoföre çabucak güvendi.";
    private static final String TEST_DATA_JAPANESE = "イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム";
    private static final String TEST_DATA_ASCII = "The quick brown fox jumps over the lazy dog";
    private static final String TEST_DATA_UTF_4_BYTE_EMOJIS = "loudly crying face:\uD83D\uDE2D nerd face: \uD83E\uDD13";
    private static final String TEST_DATA_ALL =
            TEST_DATA_TURKISH + TEST_DATA_JAPANESE + TEST_DATA_ASCII + TEST_DATA_UTF_4_BYTE_EMOJIS;
    private static final int TEST_STR_SIZE = 1 << 20;

    private static final byte[] TEST_DATA_BYTES_ALL = TEST_DATA_ALL.getBytes(StandardCharsets.UTF_8);

    private static final char[] allChars;

    private InternalSerializationService serializationService;

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
    public void testStringEncode() {
        byte[] expected = toDataByte(TEST_DATA_BYTES_ALL);
        byte[] actual = serializationService.toBytes(TEST_DATA_ALL);
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testStringDecode() {
        Data data = new HeapData(toDataByte(TEST_DATA_BYTES_ALL));
        String actualStr = serializationService.toObject(data);
        assertEquals(TEST_DATA_ALL, actualStr);
    }

    @Test
    public void testStringAllCharLetterEncode() {
        String allStr = new String(allChars);
        byte[] expected = allStr.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = serializationService.toBytes(allStr);
        byte[] actual = Arrays.copyOfRange(bytes, HeapData.DATA_OFFSET + Bits.INT_SIZE_IN_BYTES, bytes.length);
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testLargeStringEncodeDecode() {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        int j = 0;
        while (j < TEST_STR_SIZE) {
            int ch = i++ % Character.MAX_VALUE;
            if (Character.isLetter(ch)) {
                sb.append(ch);
                j++;
            }
        }
        String actualStr = sb.toString();
        byte[] strBytes = actualStr.getBytes(StandardCharsets.UTF_8);
        byte[] actualDataBytes = serializationService.toBytes(actualStr);
        byte[] expectedDataByte = toDataByte(strBytes);
        String decodedStr = serializationService.toObject(new HeapData(expectedDataByte));
        assertArrayEquals("Deserialized byte array do not match utf-8 encoding", expectedDataByte, actualDataBytes);
        assertEquals(decodedStr, actualStr);
    }

    @Test
    public void testNullStringEncodeDecode() {
        Data nullData = serializationService.toData(null);
        String decodedStr = serializationService.toObject(nullData);

        assertNull(decodedStr);
    }

    @Test
    public void testNullStringEncodeDecode2() throws Exception {
        BufferObjectDataOutput objectDataOutput = serializationService.createObjectDataOutput();
        objectDataOutput.writeString(null);

        byte[] bytes = objectDataOutput.toByteArray();
        objectDataOutput.close();

        BufferObjectDataInput objectDataInput = serializationService.createObjectDataInput(bytes);

        String decodedStr = objectDataInput.readString();
        assertNull(decodedStr);
    }

    @Test
    public void testStringAllCharLetterDecode() {
        String allStr = new String(allChars);
        byte[] expected = allStr.getBytes(StandardCharsets.UTF_8);
        Data data = new HeapData(toDataByte(expected));
        String actualStr = serializationService.toObject(data);
        assertEquals(allStr, actualStr);
    }

    @Test
    public void testStringArrayEncodeDecode() {
        String[] stringArray = new String[100];
        for (int i = 0; i < stringArray.length; i++) {
            stringArray[i] = TEST_DATA_ALL + i;
        }
        Data dataStrArray = serializationService.toData(stringArray);
        String[] actualStr = serializationService.toObject(dataStrArray);

        assertEquals(SerializationConstants.CONSTANT_TYPE_STRING_ARRAY, dataStrArray.getType());
        assertArrayEquals(stringArray, actualStr);
    }

    private byte[] toDataByte(byte[] input) {
        // the first 4 byte of type id, 4 byte string length and last 4 byte of partition hashCode
        if (serializationService.getByteOrder() == BIG_ENDIAN) {
            return toDataByteBigEndian(input);
        } else {
            return toDataByteLittleEndian(input);
        }
    }

    private byte[] toDataByteBigEndian(byte[] input) {
        ByteBuffer bf = ByteBuffer.allocate(input.length + 12);
        bf.putInt(0);
        bf.putInt(SerializationConstants.CONSTANT_TYPE_STRING);
        bf.putInt(input.length);
        bf.put(input);
        return bf.array();
    }

    private byte[] toDataByteLittleEndian(byte[] input) {
        ByteBuffer bf = ByteBuffer.allocate(input.length + 12);
        bf.order(LITTLE_ENDIAN);
        bf.putInt(0);
        // even when serialization service is configured with little endian byte order,
        // the serializerTypeId (CONSTANT_TYPE_STRING) is still output in BIG_ENDIAN
        bf.put((byte) 0xFF);
        bf.put((byte) 0xFF);
        bf.put((byte) 0xFF);
        bf.put((byte) 0xF5);
        bf.putInt(input.length);
        bf.put(input);
        return bf.array();
    }
}
