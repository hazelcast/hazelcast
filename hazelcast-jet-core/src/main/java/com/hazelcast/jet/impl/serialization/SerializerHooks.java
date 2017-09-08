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

import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.jet.TimestampedEntry;
import com.hazelcast.jet.pipeline.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.datamodel.Tuple3;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;

public final class SerializerHooks {

    /**
     * Start of reserved space for Jet-specific serializers.
     * Any ID greater than this number might be used by some other Hazelcast serializer.
     * For more information, {@see SerializationConstants}
     */
    public static final int JET_RESERVED_SPACE_START = SerializationConstants.JET_SERIALIZER_FIRST;

    public static final int MAP_ENTRY = -300;
    public static final int CUSTOM_CLASS_LOADED_OBJECT = -301;
    public static final int OBJECT_ARRAY = -302;
    public static final int TIMESTAMPED_ENTRY = -303;
    public static final int LONG_ACC = -304;
    public static final int DOUBLE_ACC = -305;
    public static final int MUTABLE_REFERENCE = -306;
    public static final int LIN_TREND_ACC = -307;
    public static final int LONG_LONG_ACC = -308;
    public static final int LONG_DOUBLE_ACC = -309;
    public static final int TUPLE2 = -310;
    public static final int TUPLE3 = -311;

    // reserved for hadoop module: -380 to -390

    /**
     * End of reserved space for Jet-specific serializers.
     * Any ID less than this number might be used by some other Hazelcast serializer.
     */
    public static final int JET_RESERVED_SPACE_END = SerializationConstants.JET_SERIALIZER_LAST;

    private SerializerHooks() {
    }

    public static final class ObjectArrayHook implements SerializerHook<Object[]> {

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

    public static final class MapEntryHook implements SerializerHook<Entry> {

        @Override
        public Class<Entry> getSerializationType() {
            return Map.Entry.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<Entry>() {
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
                    return entry(in.readObject(), in.readObject());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class TimestampedEntryHook implements SerializerHook<TimestampedEntry> {

        @Override
        public Class<TimestampedEntry> getSerializationType() {
            return TimestampedEntry.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<TimestampedEntry>() {
                @Override
                public void write(ObjectDataOutput out, TimestampedEntry object) throws IOException {
                    out.writeLong(object.getTimestamp());
                    out.writeObject(object.getKey());
                    out.writeObject(object.getValue());
                }

                @Override
                public TimestampedEntry read(ObjectDataInput in) throws IOException {
                    long timestamp = in.readLong();
                    Object key = in.readObject();
                    Object value = in.readObject();
                    return new TimestampedEntry<>(timestamp, key, value);
                }

                @Override
                public int getTypeId() {
                    return SerializerHooks.TIMESTAMPED_ENTRY;
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

    public static final class Tuple2Hook implements SerializerHook<Tuple2> {

        @Override
        public Class<Tuple2> getSerializationType() {
            return Tuple2.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<Tuple2>() {
                @Override
                public void write(ObjectDataOutput out, Tuple2 t) throws IOException {
                    out.writeObject(t.f0());
                    out.writeObject(t.f1());
                }

                @Override
                public Tuple2 read(ObjectDataInput in) throws IOException {
                    return new Tuple2<>(in.readObject(), in.readObject());
                }

                @Override
                public int getTypeId() {
                    return SerializerHooks.TUPLE2;
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

    public static final class Tuple3Hook implements SerializerHook<Tuple3> {

        @Override
        public Class<Tuple3> getSerializationType() {
            return Tuple3.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<Tuple3>() {
                @Override
                public void write(ObjectDataOutput out, Tuple3 t) throws IOException {
                    out.writeObject(t.f0());
                    out.writeObject(t.f1());
                    out.writeObject(t.f2());
                }

                @Override
                public Tuple3 read(ObjectDataInput in) throws IOException {
                    return new Tuple3<>(in.readObject(), in.readObject(), in.readObject());
                }

                @Override
                public int getTypeId() {
                    return SerializerHooks.TUPLE3;
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
