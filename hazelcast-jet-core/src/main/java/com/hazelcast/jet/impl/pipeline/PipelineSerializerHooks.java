/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.jet.impl.pipeline.JetEvent.jetEvent;

/**
 * Hazelcast serializer hooks for the classes in the {@code
 * com.hazelcast.jet.impl.pipeline} package. This is not a public API.
 */
public class PipelineSerializerHooks {

    public static final class JetEventHook implements SerializerHook<JetEvent> {

        @Override
        public Class<JetEvent> getSerializationType() {
            return JetEvent.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<JetEvent>() {
                @Override
                public void write(ObjectDataOutput out, JetEvent object) throws IOException {
                    out.writeObject(object.payload());
                    out.writeLong(object.timestamp());
                }

                @Override
                public JetEvent read(ObjectDataInput in) throws IOException {
                    Object payload = in.readObject();
                    long timestamp = in.readLong();
                    return jetEvent(payload, timestamp);
                }

                @Override
                public int getTypeId() {
                    return SerializerHookConstants.JET_EVENT;
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
