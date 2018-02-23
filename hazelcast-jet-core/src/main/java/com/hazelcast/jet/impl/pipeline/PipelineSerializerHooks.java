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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

/**
 * Hazelcast serializer hooks for the classes in the {@code
 * com.hazelcast.jet.impl.pipeline} package. This is not a public API.
 */
public class PipelineSerializerHooks {

    public static final class JetEventImplHook implements SerializerHook<JetEventImpl> {

        @Override
        public Class<JetEventImpl> getSerializationType() {
            return JetEventImpl.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<JetEventImpl>() {
                @Override
                public void write(ObjectDataOutput out, JetEventImpl object) throws IOException {
                    out.writeObject(object.payload());
                    out.writeLong(object.timestamp());
                }

                @Override
                public JetEventImpl read(ObjectDataInput in) throws IOException {
                    Object payload = in.readObject();
                    long timestamp = in.readLong();
                    return (JetEventImpl) JetEventImpl.jetEvent(payload, timestamp);
                }

                @Override
                public int getTypeId() {
                    return SerializerHookConstants.JET_EVENT_IMPL;
                }

                @Override
                public void destroy() {
                }
            };
        }

        @Override public boolean isOverwritable() {
            return false;
        }
    }

}
