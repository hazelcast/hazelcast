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

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder.DEFAULT_BYTE_ORDER;
import static com.hazelcast.internal.util.JVMUtil.upcast;
import static com.hazelcast.test.HazelcastTestSupport.ignore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DataInputNavigableJsonAdapterTest {

    private static final byte[][] SAMPLE_BYTES = new byte[][] {
            "\uD83E\uDD13".getBytes(StandardCharsets.UTF_8),
            "Bariš".getBytes(StandardCharsets.UTF_8),
            "Simple code points".getBytes(StandardCharsets.UTF_8),
            "イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム".getBytes(StandardCharsets.UTF_8),
    };

    private static final byte[][] MALFORMED_SAMPLE_BYTES = new byte[][] {
            // unexpected continuation bytes
            new byte[] {70, 65, 73, 76, 83, (byte) 0x80},
            new byte[] {(byte) 0xbf},
            // 2-byte sequence start characters followed by space
            new byte[] {(byte) 0xc0, 32},
            new byte[] {(byte) 0xc1, 32},
            new byte[] {(byte) 0xcf, 32},
            new byte[] {(byte) 0xdf, 32},
            // 3-byte sequence start characters followed by space
            new byte[] {(byte) 0xe0, 32},
            new byte[] {(byte) 0xe1, 32},
            new byte[] {(byte) 0xef, 32},
            // 4-byte sequence start characters followed by space
            new byte[] {(byte) 0xf0, 32},
            new byte[] {(byte) 0xf1, 32},
            new byte[] {(byte) 0xf4, 32},
            // non minimal sequences
            new byte[] {(byte) 0xc0, (byte) 0xaf},
            new byte[] {(byte) 0xe0, (byte) 0x9f, (byte) 0xbf},
            new byte[] {(byte) 0xf0, (byte) 0x8f, (byte) 0xbf, (byte) 0xbf},
    };

    private InternalSerializationService serializationService;
    private BufferObjectDataInput input;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void testSamplesWhenReadingOne()
            throws IOException {
        for (byte[] sample : SAMPLE_BYTES) {
            input = new ByteArrayObjectDataInput(sample, serializationService, DEFAULT_BYTE_ORDER);
            DataInputNavigableJsonAdapter.UTF8Reader reader = new DataInputNavigableJsonAdapter.UTF8Reader(input);
            assertReadOne(reader, sample);
        }
    }

    @Test
    public void testSamples()
            throws IOException {
        for (byte[] sample : SAMPLE_BYTES) {
            input = new ByteArrayObjectDataInput(sample, serializationService, DEFAULT_BYTE_ORDER);
            DataInputNavigableJsonAdapter.UTF8Reader reader = new DataInputNavigableJsonAdapter.UTF8Reader(input);
            assertReadFully(reader, sample);
        }
    }

    @Test
    public void testMalformedSamples()
            throws IOException {
        for (byte[] sample : MALFORMED_SAMPLE_BYTES) {
            input = new ByteArrayObjectDataInput(sample, serializationService, DEFAULT_BYTE_ORDER);
            DataInputNavigableJsonAdapter.UTF8Reader reader = new DataInputNavigableJsonAdapter.UTF8Reader(input);
            assertMalformed(reader);
        }
    }

    @Test
    public void testMixedRead_whenMultiReadFirst() throws IOException {
        for (byte[] sample : SAMPLE_BYTES) {
            assertReadMixed(sample, false);
        }
    }

    @Test
    public void testMixedRead_whenSingleReadFirst() throws IOException {
        for (byte[] sample : SAMPLE_BYTES) {
            assertReadMixed(sample, true);
        }
    }

    @Test
    public void testCreateAndUseConcurrently() throws InterruptedException {
        // UTF8Reader is not thread safe; the purpose of the test
        // is to ensure there is no interference across UTF8Reader instances
        int concurrency = 8;
        CountDownLatch latch = new CountDownLatch(1);
        Thread[] threads = new Thread[concurrency];
        for (int i = 0; i < concurrency; i++) {
            threads[i] = new Thread(() -> {
                try {
                    latch.await();
                    for (byte[] sample : SAMPLE_BYTES) {
                        BufferObjectDataInput input =
                                new ByteArrayObjectDataInput(sample, serializationService, DEFAULT_BYTE_ORDER);
                        DataInputNavigableJsonAdapter.UTF8Reader reader =
                                new DataInputNavigableJsonAdapter.UTF8Reader(input);
                        assertReadFully(reader, sample);
                    }
                } catch (InterruptedException e) {
                    ignore(e);
                } catch (IOException e) {
                    ExceptionUtil.rethrow(e);
                }
            });
            threads[i].start();
        }
        latch.countDown();
        for (int i = 0; i < concurrency; i++) {
            threads[i].join();
        }
    }

    private void assertReadFully(DataInputNavigableJsonAdapter.UTF8Reader reader, byte[] sample)
            throws IOException {
        String expected = new String(sample, StandardCharsets.UTF_8);
        char[] chars = new char[expected.length()];
        int charsRead = reader.read(chars, 0, chars.length);
        assertEquals(chars.length, charsRead);
        assertEquals(expected, new String(chars));
    }

    private void assertReadOne(DataInputNavigableJsonAdapter.UTF8Reader reader, byte[] sample)
            throws IOException {
        String expected = new String(sample, StandardCharsets.UTF_8);
        CharBuffer buffer = CharBuffer.allocate(expected.length() * 3);
        int next = reader.read();
        while (next != -1) {
            buffer.append((char) next);
            next = reader.read();
        }
        upcast(buffer).limit(buffer.position());
        upcast(buffer).rewind();
        String actual = buffer.toString();
        assertEquals(expected, actual);
    }

    private void assertReadMixed(byte[] sample, boolean singleReadFirst)
            throws IOException {
        String expected = new String(sample, StandardCharsets.UTF_8);
        input = new ByteArrayObjectDataInput(sample, serializationService, DEFAULT_BYTE_ORDER);
        DataInputNavigableJsonAdapter.UTF8Reader reader = new DataInputNavigableJsonAdapter.UTF8Reader(input);
        char[] chars = new char[expected.length()];
        int charsRead = 0;
        while (charsRead < expected.length()) {
            int count;
            if (charsRead % 2 == 0 ^ singleReadFirst) {
                count = reader.read(chars, charsRead, 1);
            } else {
                count = reader.read();
                if (count != -1) {
                    chars[charsRead] = (char) count;
                    count = 1;
                }
            }
            if (count == -1) {
                break;
            }
            charsRead += count;
        }
        assertEquals(chars.length, charsRead);
        assertEquals(expected, new String(chars));
    }

    private void assertMalformed(DataInputNavigableJsonAdapter.UTF8Reader reader)
            throws IOException {
        char[] chars = new char[6];
        try {
            reader.read(chars, 0, chars.length);
            fail("MalformedInputException was expected but not thrown");
        } catch (MalformedInputException e) {
            ignore(e);
        }
    }
}
