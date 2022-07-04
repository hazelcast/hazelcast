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

package com.hazelcast.jet.datamodel;

import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.datamodel.Tuple4.tuple4;
import static com.hazelcast.jet.datamodel.Tuple5.tuple5;

/**
 * Hazelcast serializer hooks for the classes in the {@code
 * com.hazelcast.jet.datamodel} package. This is not a public-facing API.
 */
final class DataModelSerializerHooks {

    private DataModelSerializerHooks() {
    }

    public static final class WindowResultHook implements SerializerHook<WindowResult> {

        @Override
        public Class<WindowResult> getSerializationType() {
            return WindowResult.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<WindowResult>() {
                @Override
                public void write(ObjectDataOutput out, WindowResult wr) throws IOException {
                    out.writeLong(wr.start());
                    out.writeLong(wr.end());
                    out.writeBoolean(wr.isEarly());
                    out.writeObject(wr.result());
                }

                @Override
                public WindowResult read(ObjectDataInput in) throws IOException {
                    long start = in.readLong();
                    long end = in.readLong();
                    boolean isEarly = in.readBoolean();
                    Object result = in.readObject();
                    return new WindowResult<>(start, end, result, isEarly);
                }

                @Override
                public int getTypeId() {
                    return SerializerHookConstants.WINDOW_RESULT;
                }
            };
        }

        @Override public boolean isOverwritable() {
            return false;
        }
    }

    public static final class KeyedWindowResultHook implements SerializerHook<KeyedWindowResult> {

        @Override
        public Class<KeyedWindowResult> getSerializationType() {
            return KeyedWindowResult.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<KeyedWindowResult>() {
                @Override
                public void write(ObjectDataOutput out, KeyedWindowResult kwr) throws IOException {
                    out.writeLong(kwr.start());
                    out.writeLong(kwr.end());
                    out.writeBoolean(kwr.isEarly());
                    out.writeObject(kwr.key());
                    out.writeObject(kwr.result());
                }

                @Override
                public KeyedWindowResult read(ObjectDataInput in) throws IOException {
                    long start = in.readLong();
                    long end = in.readLong();
                    boolean isEarly = in.readBoolean();
                    Object key = in.readObject();
                    Object result = in.readObject();
                    return new KeyedWindowResult<>(start, end, key, result, isEarly);
                }

                @Override
                public int getTypeId() {
                    return SerializerHookConstants.KEYED_WINDOW_RESULT;
                }
            };
        }

        @Override public boolean isOverwritable() {
            return false;
        }
    }

    public static final class TimestampedItemHook implements SerializerHook<TimestampedItem> {

        @Override
        public Class<TimestampedItem> getSerializationType() {
            return TimestampedItem.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<TimestampedItem>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.TIMESTAMPED_ITEM;
                }

                @Override
                public void write(ObjectDataOutput out, TimestampedItem timestampedItem) throws IOException {
                    out.writeLong(timestampedItem.timestamp());
                    out.writeObject(timestampedItem.item());

                }

                @Override
                public TimestampedItem read(ObjectDataInput in) throws IOException {
                    long timestamp = in.readLong();
                    Object item = in.readObject();
                    return new TimestampedItem<>(timestamp, item);
                }
            };
        }

        @Override
        public boolean isOverwritable() {
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
                    return tuple2(in.readObject(), in.readObject());
                }

                @Override
                public int getTypeId() {
                    return SerializerHookConstants.TUPLE2;
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
                    return tuple3(in.readObject(), in.readObject(), in.readObject());
                }

                @Override
                public int getTypeId() {
                    return SerializerHookConstants.TUPLE3;
                }
            };
        }

        @Override public boolean isOverwritable() {
            return false;
        }
    }

    public static final class Tuple4Hook implements SerializerHook<Tuple4> {

        @Override
        public Class<Tuple4> getSerializationType() {
            return Tuple4.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<Tuple4>() {
                @Override
                public void write(ObjectDataOutput out, Tuple4 t) throws IOException {
                    out.writeObject(t.f0());
                    out.writeObject(t.f1());
                    out.writeObject(t.f2());
                    out.writeObject(t.f3());
                }

                @Override
                public Tuple4 read(ObjectDataInput in) throws IOException {
                    return tuple4(in.readObject(), in.readObject(), in.readObject(), in.readObject());
                }

                @Override
                public int getTypeId() {
                    return SerializerHookConstants.TUPLE4;
                }
            };
        }

        @Override public boolean isOverwritable() {
            return false;
        }
    }

    public static final class Tuple5Hook implements SerializerHook<Tuple5> {

        @Override
        public Class<Tuple5> getSerializationType() {
            return Tuple5.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<Tuple5>() {
                @Override
                public void write(ObjectDataOutput out, Tuple5 t) throws IOException {
                    out.writeObject(t.f0());
                    out.writeObject(t.f1());
                    out.writeObject(t.f2());
                    out.writeObject(t.f3());
                    out.writeObject(t.f4());
                }

                @Override
                public Tuple5 read(ObjectDataInput in) throws IOException {
                    return tuple5(in.readObject(), in.readObject(), in.readObject(), in.readObject(), in.readObject());
                }

                @Override
                public int getTypeId() {
                    return SerializerHookConstants.TUPLE5;
                }
            };
        }

        @Override public boolean isOverwritable() {
            return false;
        }
    }

    public static final class TagHook implements SerializerHook<Tag> {

        @Override
        public Class<Tag> getSerializationType() {
            return Tag.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<Tag>() {
                @Override
                public void write(ObjectDataOutput out, Tag tag) throws IOException {
                    out.writeInt(tag.index());
                }

                @Override
                public Tag read(ObjectDataInput in) throws IOException {
                    return Tag.tag(in.readInt());
                }

                @Override
                public int getTypeId() {
                    return SerializerHookConstants.TAG;
                }
            };
        }

        @Override public boolean isOverwritable() {
            return false;
        }
    }

    public static final class ItemsByTagHook implements SerializerHook<ItemsByTag> {

        @Override
        public Class<ItemsByTag> getSerializationType() {
            return ItemsByTag.class;
        }

        @Override
        public Serializer createSerializer() {
            return new ItemsByTagSerializer();
        }

        @Override public boolean isOverwritable() {
            return false;
        }

    }

    private static class ItemsByTagSerializer implements StreamSerializer<ItemsByTag> {
        @Override
        public void write(ObjectDataOutput out, ItemsByTag ibt) throws IOException {
            Set<Entry<Tag<?>, Object>> entries = ibt.entrySet();
            out.writeInt(entries.size());
            for (Entry<Tag<?>, Object> e : entries) {
                out.writeObject(e.getKey());
                Object val = e.getValue();
                out.writeObject(val);
            }
        }

        @Override
        public ItemsByTag read(ObjectDataInput in) throws IOException {
            int size = in.readInt();
            ItemsByTag ibt = new ItemsByTag();
            for (int i = 0; i < size; i++) {
                ibt.put(in.readObject(), in.readObject());
            }
            return ibt;
        }

        @Override
        public int getTypeId() {
            return SerializerHookConstants.ITEMS_BY_TAG;
        }
    }
}
