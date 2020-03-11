/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.google.common.collect.ImmutableMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JetSerializationServiceTest {

    @Test
    public void when_triesToRegisterTwoSerializersWithSameTypeId_then_Fails() {
        // Given
        Map<Class<?>, Serializer> serializers = ImmutableMap.of(
                Boolean.class, new ObjectSerializer(),
                Byte.class, new ObjectSerializer()
        );

        // When
        // Then
        assertThatThrownBy(() -> new JetSerializationService(serializers, null))
                .isInstanceOf(IllegalStateException.class);
    }

    private static class ObjectSerializer implements StreamSerializer<Object> {

        @Override
        public int getTypeId() {
            return 1;
        }

        @Override
        public void write(ObjectDataOutput output, Object value) {
        }

        @Override
        public Object read(ObjectDataInput input) {
            return new Object();
        }

        @Override
        public void destroy() {
        }
    }
}
