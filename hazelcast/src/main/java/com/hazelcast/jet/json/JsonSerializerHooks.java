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

package com.hazelcast.jet.json;

import com.fasterxml.jackson.jr.ob.impl.DeferredMap;
import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.Map;

/**
 * Hazelcast serializer hooks for the classes in the {@code
 * com.hazelcast.jet.json} package. This is not a public-facing API.
 */
class JsonSerializerHooks {

    public static final class DeferredMapHook implements SerializerHook<DeferredMap> {

        @Override
        public Class<DeferredMap> getSerializationType() {
            return DeferredMap.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<DeferredMap>() {

                @Override
                public int getTypeId() {
                    return SerializerHookConstants.DEFERRED_MAP;
                }

                @Override
                public void write(ObjectDataOutput out, DeferredMap deferredMap) throws IOException {
                    out.writeInt(deferredMap.size());
                    for (Map.Entry<String, Object> entry : deferredMap.entrySet()) {
                        out.writeUTF(entry.getKey());
                        out.writeObject(entry.getValue());
                    }
                }

                @Override
                public DeferredMap read(ObjectDataInput in) throws IOException {
                    int size = in.readInt();
                    DeferredMap deferredMap = new DeferredMap(false, size);
                    for (int i = 0; i < size; i++) {
                        deferredMap.put(in.readUTF(), in.readObject());
                    }
                    return deferredMap;
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return false;
        }
    }
}
