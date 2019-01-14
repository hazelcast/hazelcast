/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;

public final class MapEntryHook implements SerializerHook<Entry> {

    @Override
    public Class<Entry> getSerializationType() {
        return Entry.class;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<Entry>() {
            @Override
            public int getTypeId() {
                return SerializerHookConstants.MAP_ENTRY;
            }

            @Override
            public void write(ObjectDataOutput out, Entry object) throws IOException {
                out.writeObject(object.getKey());
                out.writeObject(object.getValue());
            }

            @Override
            public Entry read(ObjectDataInput in) throws IOException {
                return entry(in.readObject(), in.readObject());
            }

            @Override
            public void destroy() {
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return true;
    }
}
