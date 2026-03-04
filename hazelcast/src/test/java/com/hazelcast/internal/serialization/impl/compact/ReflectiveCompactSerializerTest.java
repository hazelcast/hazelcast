/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.ClassFilter;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.TestAccessors;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

public class ReflectiveCompactSerializerTest {

    ReflectiveCompactSerializerTest() throws IOException {
    }

    public static class ExampleDto {
        int field;
        String text;

        public ExampleDto(int field, String text) {
            this.field = field;
            this.text = text;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ExampleDto that = (ExampleDto) o;
            return field == that.field && Objects.equals(text, that.text);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, text);
        }
    }

    InMemorySchemaService schemaService = new InMemorySchemaService();
    ExampleDto obj = new ExampleDto(1, "aaa");
    byte[] serializedObj = serialize(obj, null, null);

    @Test
    void writeWithoutRestriction() {
        assertThatNoException().isThrownBy(() -> serialize(obj, null, null));
    }

    @Test
    void writeOtherClassBlocked() {
        assertThatNoException().isThrownBy(() -> serialize(obj, otherClassFilter(), null));
    }

    @Test
    void writeClassBlockedThrows() {
        assertThatThrownBy(() -> serialize(obj, thisClassFilter(), null))
            .isInstanceOf(ReflectiveCompactSerializationUnsupportedException.class);
    }

    @Test
    void writeOtherClassAllowedThrows() {
        assertThatThrownBy(() -> serialize(obj, null, otherClassFilter()))
                .isInstanceOf(ReflectiveCompactSerializationUnsupportedException.class);
    }

    @Test
    void writeClassBlockedAndAllowedThrows() {
        assertThatThrownBy(() -> serialize(obj, thisClassFilter(), thisClassFilter()))
                .isInstanceOf(ReflectiveCompactSerializationUnsupportedException.class);
    }

    @Test
    void writeClassAllowed() {
        assertThatNoException().isThrownBy(() -> serialize(obj, null, thisClassFilter()));
    }

    @Test
    void readWithoutBlocklist() throws IOException {
        assertThat(deserialize(serializedObj, null, null)).isEqualTo(obj);
    }

    @Test
    void readOtherClassBlocked() throws IOException {
        assertThat(deserialize(serializedObj, otherClassFilter(), null)).isEqualTo(obj);
    }

    @Test
    void readClassBlockedThrows() {
        assertThatThrownBy(() -> deserialize(serializedObj, thisClassFilter(), null))
            .isInstanceOf(ReflectiveCompactSerializationUnsupportedException.class);
    }

    @Test
    void readClassBlockedAndAllowedThrows() {
        assertThatThrownBy(() -> deserialize(serializedObj, thisClassFilter(), thisClassFilter()))
                .isInstanceOf(ReflectiveCompactSerializationUnsupportedException.class);
    }

    @Test
    void readOtherClassBlockedAndThisClassAllowed() throws IOException {
        assertThat(deserialize(serializedObj, otherClassFilter(), thisClassFilter())).isEqualTo(obj);
    }

    @Test
    void readOtherClassAllowed() {
        assertThatThrownBy(() -> deserialize(serializedObj, null, otherClassFilter()))
                .isInstanceOf(ReflectiveCompactSerializationUnsupportedException.class);
    }

    @Test
    void readEmptyAllowList() {
        assertThatThrownBy(() -> deserialize(serializedObj, null, new ClassFilter()))
                .isInstanceOf(ReflectiveCompactSerializationUnsupportedException.class);
    }

    @Test
    void readClassAllowed() throws IOException {
        assertThat(deserialize(serializedObj, null, thisClassFilter())).isEqualTo(obj);
    }

    private ClassFilter otherClassFilter() {
        return new ClassFilter().addClasses("com.hazelcast.some.OtherClass");
    }

    private ClassFilter thisClassFilter() {
        return new ClassFilter().addClasses(ExampleDto.class.getName());
    }

    private byte[] serialize(Object obj, ClassFilter blockList, ClassFilter allowList) throws IOException {
        CompactStreamSerializer compactStreamSerializer = getCompactStreamSerializer(blockList, allowList);
        var out = TestAccessors.createByteArrayObjectDataOutput(mock(InternalSerializationService.class));
        compactStreamSerializer.write(out, obj);
        return out.toByteArray();
    }

    private Object deserialize(byte[] data, ClassFilter blockList, ClassFilter allowList) throws IOException {
        var in = TestAccessors.createByteArrayObjectDataInput(mock(InternalSerializationService.class), data);
        return getCompactStreamSerializer(blockList, allowList).read(in);
    }

    private CompactStreamSerializer getCompactStreamSerializer(ClassFilter blockList, ClassFilter allowList) {
        return new CompactStreamSerializer(null,
                new CompactSerializationConfig(), null, schemaService, null, blockList, allowList) {
            @Override
            public boolean canBeSerializedAsCompact(Class<?> clazz) {
                return true;
            }
        };
    }
}
