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

package com.hazelcast.jet.impl.serialization;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.internal.serialization.impl.AbstractSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.JetException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DelegatingSerializationServiceTest {

    private static final AbstractSerializationService DELEGATE = new DefaultSerializationServiceBuilder()
            .setConfig(new SerializationConfig().addSerializerConfig(
                    new SerializerConfig().setTypeClass(Value.class).setClass(ValueSerializer.class))
            ).build();

    private static final int TYPE_ID = 1;

    @Test
    public void when_triesToRegisterSerializerWithNegativeTypeId_then_Fails() {
        // Given
        Map<Class<?>, Serializer> serializers = ImmutableMap.of(
                Object.class, new StreamSerializer<Object>() {
                    @Override
                    public int getTypeId() {
                        return -1;
                    }

                    @Override
                    public void write(ObjectDataOutput objectDataOutput, Object o) {
                    }

                    @Override
                    public Value read(ObjectDataInput objectDataInput) {
                        return null;
                    }
                }
        );

        // When
        // Then
        assertThatThrownBy(() -> new DelegatingSerializationService(serializers, DELEGATE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void when_triesToRegisterTwoSerializersWithSameTypeId_then_Fails() {
        // Given
        Map<Class<?>, Serializer> serializers = ImmutableMap.of(
                int.class, new ValueSerializer(),
                long.class, new ValueSerializer()
        );

        // When
        // Then
        assertThatThrownBy(() -> new DelegatingSerializationService(serializers, DELEGATE))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void when_triesToFindSerializerForUnregisteredType_then_Fails() {
        // Given
        DelegatingSerializationService service = new DelegatingSerializationService(emptyMap(), DELEGATE);

        // When
        // Then
        assertThatThrownBy(() -> service.serializerFor(new Object(), false))
                .isInstanceOf(JetException.class);
        assertThatThrownBy(() -> service.serializerFor(Integer.MAX_VALUE))
                .isInstanceOf(JetException.class);
    }

    @Test
    public void when_doesNotFind_then_Delegates() {
        // Given
        DelegatingSerializationService service = new DelegatingSerializationService(emptyMap(), DELEGATE);

        // When
        // Then
        assertThat(service.serializerFor(TYPE_ID).getImpl()).isInstanceOf(ValueSerializer.class);
        assertThat(service.serializerFor(new Value(), false).getImpl()).isInstanceOf(ValueSerializer.class);
    }

    @Test
    public void when_multipleTypeSerializersRegistered_then_localHasPrecedence() {
        // Given
        Serializer serializer = new ValueSerializer();
        Map<Class<?>, Serializer> serializers = ImmutableMap.of(Byte.class, serializer);

        DelegatingSerializationService service = new DelegatingSerializationService(serializers, DELEGATE);

        // When
        // Then
        assertThat(service.serializerFor(TYPE_ID).getImpl()).isEqualTo(serializer);
        assertThat(service.serializerFor(Byte.valueOf((byte) 1), false).getImpl()).isEqualTo(serializer);
    }

    @Test
    public void when_triesToFindSerializerForNullObject_then_Succeeds() {
        // Given
        DelegatingSerializationService service = new DelegatingSerializationService(emptyMap(), DELEGATE);

        // When
        // Then
        assertThat(service.serializerFor(null, false).getImpl()).isNotNull();
    }

    private static class Value {
    }

    private static class ValueSerializer implements StreamSerializer<Value> {

        @Override
        public int getTypeId() {
            return TYPE_ID;
        }

        @Override
        public void write(ObjectDataOutput output, Value value) {
        }

        @Override
        public Value read(ObjectDataInput input) {
            return new Value();
        }
    }
}
