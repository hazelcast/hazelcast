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

package com.hazelcast.jet.avro;

import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.apache.avro.util.Utf8;

import java.io.IOException;

/**
 * Hazelcast serializer hooks for the classes in the {@code
 * com.hazelcast.jet.avrp} package. This is not a public-facing API.
 */
class AvroSerializerHooks {

    public static final class Utf8Hook implements SerializerHook<Utf8> {

        @Override
        public Class<Utf8> getSerializationType() {
            return Utf8.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<Utf8>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.AVRO_UTF8;
                }

                @Override
                public void write(ObjectDataOutput out, Utf8 object) throws IOException {
                    out.writeByteArray(object.getBytes());
                }

                @Override
                public Utf8 read(ObjectDataInput in) throws IOException {
                    return new Utf8(in.readByteArray());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return false;
        }
    }
}
