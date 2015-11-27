/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.SerializationService;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractSerializationCompatibilityTest {

    protected SerializationService serializationService;

    @Test
    public void testSampleEncodeDecode() throws IOException {
        SerializationV1Dataserializable testData = SerializationV1Dataserializable.createInstanceWithNonNullFields();
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
        SerializationV1Portable testData = SerializationV1Portable.createInstanceWithNonNullFields();
        Data data = serializationService.toData(testData);
        SerializationV1Portable testDataFromSerializer = serializationService.toObject(data);

        assertTrue(testData.equals(testDataFromSerializer));
    }

    @Test
    public void testSamplePortableEncodeDecode_with_null_arrays() throws IOException {
        SerializationV1Portable testDataw = SerializationV1Portable.createInstanceWithNonNullFields();
        serializationService.toData(testDataw);

        SerializationV1Portable testData = new SerializationV1Portable();
        Data data = serializationService.toData(testData);
        SerializationV1Portable testDataFromSerializer = serializationService.toObject(data);

        assertEquals(testData, testDataFromSerializer);
    }
}
