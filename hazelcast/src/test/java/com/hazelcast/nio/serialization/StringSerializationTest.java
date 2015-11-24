/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.hazelcast.nio.serialization;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.ringbuffer.impl.client.HeadSequenceRequest;
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

    private SerializationService serializationService;

    private final static String TEST_DATA_TURKISH = "Pijamalı hasta, yağız şoföre çabucak güvendi.";
    private final static String TEST_DATA_JAPANESE = "イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム";
    private final static String TEST_DATA_ASCII = "The quick brown fox jumps over the lazy dog";
    private final static String TEST_DATA_ALL = TEST_DATA_TURKISH + TEST_DATA_JAPANESE + TEST_DATA_ASCII;
    private final static int TEST_STR_SIZE = 1 << 20;

    private final static byte[] TEST_DATA_BYTES_ALL = TEST_DATA_ALL.getBytes(Charset.forName("utf8"));

    private static char[] allChars;

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
        serializationService.destroy();
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
        byte[] actual = Arrays.copyOfRange(bytes, HeapData.DATA_OFFSET + Bits.INT_SIZE_IN_BYTES, bytes.length );
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
        String[] stringArray = new String[TEST_STR_SIZE];
        for (int i = 0; i < stringArray.length; i++) {
            stringArray[i] = TEST_DATA_ALL;
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
