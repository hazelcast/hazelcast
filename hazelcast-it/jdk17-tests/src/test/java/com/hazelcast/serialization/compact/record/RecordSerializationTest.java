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

package com.hazelcast.serialization.compact.record;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.compact.CompactTestUtil;
import com.hazelcast.internal.serialization.impl.compact.SchemaService;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RecordSerializationTest extends HazelcastTestSupport {

    private final SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
    private InternalSerializationService service;

    @Before
    public void createSerializationService() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.setEnabled(true);
        service = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig().setCompactSerializationConfig(compactSerializationConfig))
                .build();
    }

    @Test
    public void shouldSerializeAndDeserializeRecord() {
        AllTypesRecord allTypesRecord = AllTypesRecord.create();

        Data serialized = service.toData(allTypesRecord);
        AllTypesRecord deserialized = service.toObject(serialized);
        assertThat(deserialized)
                .isEqualTo(allTypesRecord);
    }

    @Test
    public void shouldReadOnSchemaEvolution() {
        // Let's assume all fields are deleted, and a new field is added.
        // We should still be able to read the record on our side, with
        // default values of the field types. The newly added field
        // will be ignored.
        GenericRecord evolvedRecord = GenericRecordBuilder.compact(AllTypesRecord.class.getName())
                .setString("newField", "newValue")
                .build();

        Data serialized = service.toData(evolvedRecord);
        AllTypesRecord deserialized = service.toObject(serialized);
        assertThat(deserialized)
                .isEqualTo(AllTypesRecord.createWithDefaultValues());
    }

    @Test
    public void shouldBeInteroperableBetweenNullableAndPrimitiveFixSizedTypes() {
        // Writing fix sized primitive field and reading them as nullable
        // fix sized field and vice-versa.
        GenericRecord genericRecord = GenericRecordBuilder.compact(AllTypesRecord.class.getName())
                .setInt8("objectInt8", Byte.MAX_VALUE)
                .setNullableInt8("primitiveInt8", Byte.MIN_VALUE)
                .setInt16("objectInt16", Short.MAX_VALUE)
                .setNullableInt16("primitiveInt16", Short.MIN_VALUE)
                .setInt32("objectInt32", Integer.MAX_VALUE)
                .setNullableInt32("primitiveInt32", Integer.MIN_VALUE)
                .setInt64("objectInt64", Long.MAX_VALUE)
                .setNullableInt64("primitiveInt64", Long.MIN_VALUE)
                .setFloat32("objectFloat32", Float.MAX_VALUE)
                .setNullableFloat32("primitiveFloat32", Float.MIN_VALUE)
                .setFloat64("objectFloat64", Double.MAX_VALUE)
                .setNullableFloat64("primitiveFloat64", Double.MIN_VALUE)
                .setBoolean("objectBoolean", Boolean.TRUE)
                .setNullableBoolean("primitiveBoolean", Boolean.FALSE)
                .setArrayOfInt8("objectInt8Array", new byte[]{Byte.MAX_VALUE})
                .setArrayOfNullableInt8("primitiveInt8Array", new Byte[]{Byte.MIN_VALUE})
                .setArrayOfInt16("objectInt16Array", new short[]{Short.MAX_VALUE})
                .setArrayOfNullableInt16("primitiveInt16Array", new Short[]{Short.MIN_VALUE})
                .setArrayOfInt32("objectInt32Array", new int[]{Integer.MAX_VALUE})
                .setArrayOfNullableInt32("primitiveInt32Array", new Integer[]{Integer.MIN_VALUE})
                .setArrayOfInt64("objectInt64Array", new long[]{Long.MAX_VALUE})
                .setArrayOfNullableInt64("primitiveInt64Array", new Long[]{Long.MIN_VALUE})
                .setArrayOfFloat32("objectFloat32Array", new float[]{Float.MAX_VALUE})
                .setArrayOfNullableFloat32("primitiveFloat32Array", new Float[]{Float.MIN_VALUE})
                .setArrayOfFloat64("objectFloat64Array", new double[]{Double.MAX_VALUE})
                .setArrayOfNullableFloat64("primitiveFloat64Array", new Double[]{Double.MIN_VALUE})
                .setArrayOfBoolean("objectBooleanArray", new boolean[]{Boolean.TRUE})
                .setArrayOfNullableBoolean("primitiveBooleanArray", new Boolean[]{Boolean.FALSE})
                .build();

        Data serialized = service.toData(genericRecord);
        AllTypesRecord deserialized = service.toObject(serialized);

        assertThat(deserialized.primitiveInt8())
                .isEqualTo(genericRecord.getNullableInt8("primitiveInt8"));
        assertThat(deserialized.objectInt8())
                .isEqualTo(genericRecord.getInt8("objectInt8"));
        assertThat(deserialized.primitiveInt16())
                .isEqualTo(genericRecord.getNullableInt16("primitiveInt16"));
        assertThat(deserialized.objectInt16())
                .isEqualTo(genericRecord.getInt16("objectInt16"));
        assertThat(deserialized.primitiveInt32())
                .isEqualTo(genericRecord.getNullableInt32("primitiveInt32"));
        assertThat(deserialized.objectInt32())
                .isEqualTo(genericRecord.getInt32("objectInt32"));
        assertThat(deserialized.primitiveInt64())
                .isEqualTo(genericRecord.getNullableInt64("primitiveInt64"));
        assertThat(deserialized.objectInt64())
                .isEqualTo(genericRecord.getInt64("objectInt64"));
        assertThat(deserialized.primitiveFloat32())
                .isEqualTo(genericRecord.getNullableFloat32("primitiveFloat32"));
        assertThat(deserialized.objectFloat32())
                .isEqualTo(genericRecord.getFloat32("objectFloat32"));
        assertThat(deserialized.primitiveFloat64())
                .isEqualTo(genericRecord.getNullableFloat64("primitiveFloat64"));
        assertThat(deserialized.objectFloat64())
                .isEqualTo(genericRecord.getFloat64("objectFloat64"));
        assertThat(deserialized.primitiveBoolean())
                .isEqualTo(genericRecord.getNullableBoolean("primitiveBoolean"));
        assertThat(deserialized.objectBoolean())
                .isEqualTo(genericRecord.getBoolean("objectBoolean"));

        assertThat(deserialized.primitiveInt8Array())
                .containsExactly(genericRecord.getArrayOfNullableInt8("primitiveInt8Array")[0]);
        assertThat(deserialized.objectInt8Array())
                .containsExactly(genericRecord.getArrayOfInt8("objectInt8Array")[0]);
        assertThat(deserialized.primitiveInt16Array())
                .containsExactly(genericRecord.getArrayOfNullableInt16("primitiveInt16Array")[0]);
        assertThat(deserialized.objectInt16Array())
                .containsExactly(genericRecord.getArrayOfInt16("objectInt16Array")[0]);
        assertThat(deserialized.primitiveInt32Array())
                .containsExactly(genericRecord.getArrayOfNullableInt32("primitiveInt32Array")[0]);
        assertThat(deserialized.objectInt32Array())
                .containsExactly(genericRecord.getArrayOfInt32("objectInt32Array")[0]);
        assertThat(deserialized.primitiveInt64Array())
                .containsExactly(genericRecord.getArrayOfNullableInt64("primitiveInt64Array")[0]);
        assertThat(deserialized.objectInt64Array())
                .containsExactly(genericRecord.getArrayOfInt64("objectInt64Array")[0]);
        assertThat(deserialized.primitiveFloat32Array())
                .containsExactly(genericRecord.getArrayOfNullableFloat32("primitiveFloat32Array")[0]);
        assertThat(deserialized.objectFloat32Array())
                .containsExactly(genericRecord.getArrayOfFloat32("objectFloat32Array")[0]);
        assertThat(deserialized.primitiveFloat64Array())
                .containsExactly(genericRecord.getArrayOfNullableFloat64("primitiveFloat64Array")[0]);
        assertThat(deserialized.objectFloat64Array())
                .containsExactly(genericRecord.getArrayOfFloat64("objectFloat64Array")[0]);
        assertThat(deserialized.primitiveBooleanArray())
                .containsExactly(genericRecord.getArrayOfNullableBoolean("primitiveBooleanArray")[0]);
        assertThat(deserialized.objectBooleanArray())
                .containsExactly(genericRecord.getArrayOfBoolean("objectBooleanArray")[0]);
    }

    @Test
    public void shouldThrowWhileSerializingRecordWithCharField() {
        CharRecord charRecord = new CharRecord('x');
        assertThatThrownBy(() -> service.toData(charRecord))
                .hasRootCauseInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("does not support fields of type 'char'");
    }

    @Test
    public void shouldThrowWhileSerializingRecordWithCharacterField() {
        CharacterRecord characterRecord = new CharacterRecord('x');
        assertThatThrownBy(() -> service.toData(characterRecord))
                .hasRootCauseInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("does not support fields of type 'Character'");
    }

    @Test
    public void shouldThrowWhileSerializingRecordWithCharArrayField() {
        CharArrayRecord charArrayRecord = new CharArrayRecord(new char[]{'x'});
        assertThatThrownBy(() -> service.toData(charArrayRecord))
                .hasRootCauseInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("does not support fields of type 'char[]'");
    }

    @Test
    public void shouldThrowWhileSerializingRecordWithCharacterArrayField() {
        CharacterArrayRecord characterArrayRecord = new CharacterArrayRecord(new Character[]{'x'});
        assertThatThrownBy(() -> service.toData(characterArrayRecord))
                .hasRootCauseInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("does not support fields of type 'Character[]'");
    }

    record CharRecord(char field) {
    }

    record CharacterRecord(Character field) {
    }

    record CharArrayRecord(char[] field) {
    }

    record CharacterArrayRecord(Character[] field) {
    }
}
