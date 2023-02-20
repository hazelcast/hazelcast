/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SchemaWriterTest {

    @Test
    public void testSchemaWriter() {
        SchemaWriter writer = new SchemaWriter("foo");

        writer.writeBoolean(FieldKind.BOOLEAN.name(), true);
        writer.writeArrayOfBoolean(FieldKind.ARRAY_OF_BOOLEAN.name(), null);

        writer.writeInt8(FieldKind.INT8.name(), (byte) 0);
        writer.writeArrayOfInt8(FieldKind.ARRAY_OF_INT8.name(), null);

        writer.writeInt16(FieldKind.INT16.name(), (short) 0);
        writer.writeArrayOfInt16(FieldKind.ARRAY_OF_INT16.name(), null);

        writer.writeInt32(FieldKind.INT32.name(), 0);
        writer.writeArrayOfInt32(FieldKind.ARRAY_OF_INT32.name(), null);

        writer.writeInt64(FieldKind.INT64.name(), 0);
        writer.writeArrayOfInt64(FieldKind.ARRAY_OF_INT64.name(), null);

        writer.writeFloat32(FieldKind.FLOAT32.name(), 0);
        writer.writeArrayOfFloat32(FieldKind.ARRAY_OF_FLOAT32.name(), null);

        writer.writeFloat64(FieldKind.FLOAT64.name(), 0);
        writer.writeArrayOfFloat64(FieldKind.ARRAY_OF_FLOAT64.name(), null);

        writer.writeString(FieldKind.STRING.name(), null);
        writer.writeArrayOfString(FieldKind.ARRAY_OF_STRING.name(), null);

        writer.writeDecimal(FieldKind.DECIMAL.name(), null);
        writer.writeArrayOfDecimal(FieldKind.ARRAY_OF_DECIMAL.name(), null);

        writer.writeTime(FieldKind.TIME.name(), null);
        writer.writeArrayOfTime(FieldKind.ARRAY_OF_TIME.name(), null);

        writer.writeDate(FieldKind.DATE.name(), null);
        writer.writeArrayOfDate(FieldKind.ARRAY_OF_DATE.name(), null);

        writer.writeTimestamp(FieldKind.TIMESTAMP.name(), null);
        writer.writeArrayOfTimestamp(FieldKind.ARRAY_OF_TIMESTAMP.name(), null);

        writer.writeTimestampWithTimezone(FieldKind.TIMESTAMP_WITH_TIMEZONE.name(), null);
        writer.writeArrayOfTimestampWithTimezone(FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE.name(), null);

        writer.writeCompact(FieldKind.COMPACT.name(), null);
        writer.writeArrayOfCompact(FieldKind.ARRAY_OF_COMPACT.name(), null);

        writer.writeNullableBoolean(FieldKind.NULLABLE_BOOLEAN.name(), null);
        writer.writeArrayOfNullableBoolean(FieldKind.ARRAY_OF_NULLABLE_BOOLEAN.name(), null);

        writer.writeNullableInt8(FieldKind.NULLABLE_INT8.name(), null);
        writer.writeArrayOfNullableInt8(FieldKind.ARRAY_OF_NULLABLE_INT8.name(), null);

        writer.writeNullableInt16(FieldKind.NULLABLE_INT16.name(), null);
        writer.writeArrayOfNullableInt16(FieldKind.ARRAY_OF_NULLABLE_INT16.name(), null);

        writer.writeNullableInt32(FieldKind.NULLABLE_INT32.name(), null);
        writer.writeArrayOfNullableInt32(FieldKind.ARRAY_OF_NULLABLE_INT32.name(), null);

        writer.writeNullableInt64(FieldKind.NULLABLE_INT64.name(), null);
        writer.writeArrayOfNullableInt64(FieldKind.ARRAY_OF_NULLABLE_INT64.name(), null);

        writer.writeNullableFloat32(FieldKind.NULLABLE_FLOAT32.name(), null);
        writer.writeArrayOfNullableFloat32(FieldKind.ARRAY_OF_NULLABLE_FLOAT32.name(), null);

        writer.writeNullableFloat64(FieldKind.NULLABLE_FLOAT64.name(), null);
        writer.writeArrayOfNullableFloat64(FieldKind.ARRAY_OF_NULLABLE_FLOAT64.name(), null);

        Schema schema = writer.build();

        for (FieldKind kind : FieldKind.values()) {
            if (!isSupportedByCompact(kind)) {
                continue;
            }

            FieldDescriptor field = schema.getField(kind.name());
            assertNotNull(field);

            assertEquals(kind.name(), field.getFieldName());
            assertEquals(kind, field.getKind());
        }
    }

    @Test
    public void testSchemaWriter_withDuplicateFields() {
        SchemaWriter writer = new SchemaWriter("foo");
        writer.writeInt32("bar", 0);
        assertThatThrownBy(() -> writer.writeString("bar", null))
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageContaining("already exists");
    }

    private boolean isSupportedByCompact(FieldKind kind) {
        switch (kind) {
            case NOT_AVAILABLE:
            case CHAR:
            case ARRAY_OF_CHAR:
            case PORTABLE:
            case ARRAY_OF_PORTABLE:
                return false;
        }
        return true;
    }
}
