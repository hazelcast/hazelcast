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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RabinFingerprintTest {

    @Test
    public void testRabinFingerprintIsConsistentWithWrittenData() throws IOException {
        SchemaWriter writer = new SchemaWriter("typeName");
        writer.addField(new FieldDescriptor("a", FieldKind.BOOLEAN));
        writer.addField(new FieldDescriptor("b", FieldKind.ARRAY_OF_BOOLEAN));
        writer.addField(new FieldDescriptor("c", FieldKind.TIMESTAMP_WITH_TIMEZONE));
        Schema schema = writer.build();

        InternalSerializationService internalSerializationService = new DefaultSerializationServiceBuilder()
                .setByteOrder(ByteOrder.LITTLE_ENDIAN).build();
        BufferObjectDataOutput output = internalSerializationService.createObjectDataOutput();
        schema.writeData(output);
        long fingerprint64 = RabinFingerprint.fingerprint64(output.toByteArray());
        assertEquals(fingerprint64, schema.getSchemaId());
    }
}
