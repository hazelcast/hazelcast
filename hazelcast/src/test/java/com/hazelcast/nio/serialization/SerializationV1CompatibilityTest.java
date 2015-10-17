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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SerializationV1CompatibilityTest {

    private SerializationService serializationService;

    @Before
    public void setup() {
        DefaultSerializationServiceBuilder defaultSerializationServiceBuilder = new DefaultSerializationServiceBuilder();
        serializationService = defaultSerializationServiceBuilder
                .setVersion(SerializationService.VERSION_1)
                .addPortableFactory(TestSerializationConstants.PORTABLE_FACTORY_ID, new PortableTest.TestPortableFactory())
                .build();
    }

    @After
    public void tearDown() {
        serializationService.destroy();
    }

    @Test
    public void testSampleEncodeDecode() throws IOException {
        SerializationV1Dataserializable testData = new SerializationV1Dataserializable((byte) 99, true, 'c', (short) 11, 1234134,
                1341431221l, 1.12312f, 432.424, new byte[]{(byte) 1, (byte) 2, (byte) 3}, new boolean[]{true, false, true},
                new char[]{'a', 'b', 'c'}, new short[]{1, 2, 3}, new int[]{4, 2, 3}, new long[]{11, 2, 3},
                new float[]{1.0f, 2.1f, 3.4f}, new double[]{11.1, 22.2, 33.3}, "the string text",
                new String[]{"item1", "item2", "item3"});
        Data data = serializationService.toData(testData);
        SerializationV1Dataserializable testDataFromSerializer = serializationService.toObject(data);

        assertTrue(testData.equals(testDataFromSerializer));
    }

    @Test
    public void testSampleEncodeDecode_with_null_arrays() throws IOException {
        SerializationV1Dataserializable testData = new SerializationV1Dataserializable();
        Data data = serializationService.toData(testData);
        SerializationV1Dataserializable testDataFromSerializer = serializationService.toObject(data);

        assertEquals(testData, testDataFromSerializer);
    }


    @Test
    public void testSamplePortableEncodeDecode() throws IOException {
        SerializationV1Portable testData = new SerializationV1Portable((byte) 99, true, 'c', (short) 11, 1234134,
                1341431221l, 1.12312f, 432.424, new byte[]{(byte) 1, (byte) 2, (byte) 3}, new boolean[]{true, false, true},
                new char[]{'a', 'b', 'c'}, new short[]{1, 2, 3}, new int[]{4, 2, 3}, new long[]{11, 2, 3},
                new float[]{1.0f, 2.1f, 3.4f}, new double[]{11.1, 22.2, 33.3}, "the string text",
                new String[]{"item1", "item2", "item3"});
        Data data = serializationService.toData(testData);
        SerializationV1Portable testDataFromSerializer = serializationService.toObject(data);

        assertTrue(testData.equals(testDataFromSerializer));
    }

    @Test
    public void testSamplePortableEncodeDecode_with_null_arrays() throws IOException {
        SerializationV1Portable testDataw = new SerializationV1Portable((byte) 99, true, 'c', (short) 11, 1234134,
                1341431221l, 1.12312f, 432.424, new byte[]{(byte) 1, (byte) 2, (byte) 3}, new boolean[]{true, false, true},
                new char[]{'a', 'b', 'c'}, new short[]{1, 2, 3}, new int[]{4, 2, 3}, new long[]{11, 2, 3},
                new float[]{1.0f, 2.1f, 3.4f}, new double[]{11.1, 22.2, 33.3}, "the string text",
                new String[]{"item1", "item2", "item3"});

        serializationService.toData(testDataw);

        SerializationV1Dataserializable testData = new SerializationV1Dataserializable();
        Data data = serializationService.toData(testData);
        SerializationV1Dataserializable testDataFromSerializer = serializationService.toObject(data);

        assertEquals(testData, testDataFromSerializer);
    }


}
