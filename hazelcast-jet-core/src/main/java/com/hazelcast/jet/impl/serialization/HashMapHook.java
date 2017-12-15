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

package com.hazelcast.jet.impl.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class HashMapHook implements SerializerHook<HashMap> {

    @Override
    public Class<HashMap> getSerializationType() {
        return HashMap.class;
    }

    @Override
    @SuppressWarnings("checkstyle:anoninnerlength")
    public Serializer createSerializer() {
        return new StreamSerializer<HashMap>() {

            @Override
            public int getTypeId() {
                return SerializerHookConstants.HASH_MAP;
            }

            @Override
            public void destroy() {
            }

            @Override
            @SuppressWarnings("checkstyle:illegaltype")
            public void write(ObjectDataOutput out, HashMap map) throws IOException {
                out.writeInt(map.size());
                for (Object o : map.entrySet()) {
                    Map.Entry e = (Map.Entry) o;
                    out.writeObject(e.getKey());
                    out.writeObject(e.getValue());
                }
            }

            @Override
            @SuppressWarnings("checkstyle:illegaltype")
            public HashMap read(ObjectDataInput in) throws IOException {
                int length = in.readInt();
                HashMap map = new HashMap();
                for (int i = 0; i < length; i++) {
                    //noinspection unchecked
                    map.put(in.readObject(), in.readObject());
                }
                return map;
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return true;
    }
}
