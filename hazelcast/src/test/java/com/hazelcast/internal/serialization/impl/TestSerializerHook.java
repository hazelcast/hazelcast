/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

/**
 * Test example of a serializer hook
 */
public class TestSerializerHook implements SerializerHook {

    public TestSerializerHook() {
    }

    @Override
    public Class getSerializationType() {
        return SampleIdentifiedDataSerializable.class;
    }

    @Override
    public Serializer createSerializer() {
        return new TestSerializer();
    }

    @Override
    public boolean isOverwritable() {
        return true;
    }

    public static class TestSerializer implements StreamSerializer<SampleIdentifiedDataSerializable> {
        @Override
        public int getTypeId() {
            return 1000;
        }

        @Override
        public void destroy() {

        }

        @Override
        public void write(ObjectDataOutput out, SampleIdentifiedDataSerializable object) throws IOException {

        }

        @Override
        public SampleIdentifiedDataSerializable read(ObjectDataInput in) throws IOException {
            return null;
        }
    }

    public static class TestSerializerWithTypeConstructor implements StreamSerializer {

        private Class<?> clazz;

        public TestSerializerWithTypeConstructor(Class<?> clazz) {
            this.clazz = clazz;
        }

        public TestSerializerWithTypeConstructor() {
        }

        public Class<?> getClazz() {
            return clazz;
        }

        @Override
        public int getTypeId() {
            return 1001;
        }

        @Override
        public void destroy() {

        }

        @Override
        public void write(ObjectDataOutput out, Object object) throws IOException {

        }

        @Override
        public Object read(ObjectDataInput in) throws IOException {
            return null;
        }
    }

}
