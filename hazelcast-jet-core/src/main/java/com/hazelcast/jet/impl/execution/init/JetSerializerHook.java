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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.Map.Entry;

public final class JetSerializerHook {

    public static final int MAP_ENTRY = -300;
    public static final int CUSTOM_CLASS_LOADED_OBJECT = -301;
    public static final int OBJECT_ARRAY = -302;

    private JetSerializerHook() {
    }

    public static final class ObjectArray implements SerializerHook<Object[]> {

        @Override
        public Class<Object[]> getSerializationType() {
            return Object[].class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<Object[]>() {

                @Override
                public int getTypeId() {
                    return OBJECT_ARRAY;
                }

                @Override
                public void destroy() {

                }

                @Override
                public void write(ObjectDataOutput out, Object[] array) throws IOException {
                    out.writeInt(array.length);
                    for (int i = 0; i < array.length; i++) {
                        out.writeObject(array[i]);
                    }
                }

                @Override
                public Object[] read(ObjectDataInput in) throws IOException {
                    int length = in.readInt();
                    Object[] array = new Object[length];
                    for (int i = 0; i < array.length; i++) {
                        array[i] = in.readObject();
                    }
                    return array;
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class MapEntry implements SerializerHook<Map.Entry> {

        @Override
        public Class<Entry> getSerializationType() {
            return Map.Entry.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<Map.Entry>() {
                @Override
                public int getTypeId() {
                    return MAP_ENTRY;
                }

                @Override
                public void destroy() {

                }

                @Override
                public void write(ObjectDataOutput out, Entry object) throws IOException {
                    out.writeObject(object.getKey());
                    out.writeObject(object.getValue());
                }

                @Override
                public Entry read(ObjectDataInput in) throws IOException {
                    Object key = in.readObject();
                    Object value = in.readObject();
                    return new SimpleImmutableEntry(key, value);
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }
}
