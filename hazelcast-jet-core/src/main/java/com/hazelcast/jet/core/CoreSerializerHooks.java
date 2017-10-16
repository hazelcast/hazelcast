/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

/**
 * Hazelcast serializer hooks for the classes in the {@code
 * com.hazelcast.jet.core} package. This is not a public-facing API.
 */
class CoreSerializerHooks {
    public static final class WatermarkHook implements SerializerHook<Watermark> {

        @Override
        public Class<Watermark> getSerializationType() {
            return Watermark.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<Watermark>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.WATERMARK;
                }

                @Override
                public void destroy() {
                }

                @Override
                public void write(ObjectDataOutput out, Watermark object) throws IOException {
                    out.writeLong(object.timestamp());
                }

                @Override
                public Watermark read(ObjectDataInput in) throws IOException {
                    return new Watermark(in.readLong());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }
}
