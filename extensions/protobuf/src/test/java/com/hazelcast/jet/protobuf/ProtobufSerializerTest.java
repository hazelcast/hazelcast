/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.protobuf;

import com.google.protobuf.GeneratedMessageV3;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.jet.protobuf.Messages.Person;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProtobufSerializerTest {

    private static final InternalSerializationService SERIALIZATION_SERVICE =
            new DefaultSerializationServiceBuilder().build();

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void when_instantiatedWithArbitraryClass_then_throws() {
        // When
        // Then
        assertThatThrownBy(() -> new ProtobufSerializer(Object.class, 1) { })
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void when_instantiatedWithTypeId_then_returnsIt() {
        // Given
        int typeId = 13;

        // When
        StreamSerializer<Person> serializer = ProtobufSerializer.from(Person.class, typeId);

        // Then
        assertThat(serializer.getTypeId()).isEqualTo(typeId);
    }

    @Test
    public void when_serializes_then_isAbleToDeserialize() {
        // Given
        Person original = Person.newBuilder().setName("Joe").setAge(18).build();
        StreamSerializer<Person> serializer = ProtobufSerializer.from(Person.class, 1);

        // When
        Person transformed = deserialize(serializer, serialize(serializer, original));

        // Then
        assertThat(transformed).isEqualTo(original);
    }

    private static <T extends GeneratedMessageV3> byte[] serialize(StreamSerializer<T> serializer, T object) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectDataOutputStream output = new ObjectDataOutputStream(baos, SERIALIZATION_SERVICE)) {
            serializer.write(output, object);
            return baos.toByteArray();
        } catch (IOException ioe) {
            throw sneakyThrow(ioe);
        }
    }

    private static <T extends GeneratedMessageV3> T deserialize(StreamSerializer<T> serializer, byte[] bytes) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             ObjectDataInputStream input = new ObjectDataInputStream(bais, SERIALIZATION_SERVICE)) {
            return serializer.read(input);
        } catch (IOException ioe) {
            throw sneakyThrow(ioe);
        }
    }
}
