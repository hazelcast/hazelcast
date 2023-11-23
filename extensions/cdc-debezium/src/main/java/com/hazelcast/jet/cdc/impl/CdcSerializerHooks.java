/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cdc.impl;

import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.RecordPart;
import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;

/**
 * Hazelcast serializer hooks for data objects involved in processing
 * change data capture streams.
 */
@SuppressWarnings("NullableProblems")
public class CdcSerializerHooks {

    public static final class ChangeRecordImplHook implements SerializerHook<ChangeRecordImpl> {

        @Override
        public Class<ChangeRecordImpl> getSerializationType() {
            return ChangeRecordImpl.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<ChangeRecordImpl>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.CDC_RECORD;
                }

                @Override
                public void write(ObjectDataOutput out, ChangeRecordImpl record) throws IOException {
                    out.writeLong(record.timestamp());
                    out.writeLong(record.sequenceSource());
                    out.writeLong(record.sequenceValue());
                    out.writeString(record.operation().code());
                    out.writeString(record.getKeyJson());
                    RecordPart oldValue = record.oldValue();
                    out.writeString(oldValue == null ? null : oldValue.toJson());
                    RecordPart newValue = record.newValue();
                    out.writeString(newValue == null ? null : newValue.toJson());
                    out.writeString(record.table());
                    out.writeString(record.schema());
                    out.writeString(record.database());
                }

                @Override
                public ChangeRecordImpl read(ObjectDataInput in) throws IOException {
                    long timestamp = in.readLong();
                    long sequenceSource = in.readLong();
                    long sequenceValue = in.readLong();
                    Operation operation = Operation.get(in.readString());
                    String keyJson = requireNonNull(in.readString(), "keyJson cannot be null");
                    String oldValueJson = in.readString();
                    String newValueJson = in.readString();
                    String table = in.readString();
                    String schema = in.readString();
                    String database = in.readString();
                    return new ChangeRecordImpl(
                            timestamp, sequenceSource, sequenceValue,
                            operation, keyJson, oldValueJson, newValueJson,
                            table, schema, database);
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class RecordPartImplHook implements SerializerHook<RecordPartImpl> {
        @Override
        public Class<RecordPartImpl> getSerializationType() {
            return RecordPartImpl.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<RecordPartImpl>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.CDC_RECORD_PART;
                }

                @Override
                public void write(ObjectDataOutput out, RecordPartImpl part) throws IOException {
                    out.writeString(part.toJson());
                }

                @Override
                public RecordPartImpl read(ObjectDataInput in) throws IOException {
                    String json = requireNonNull(in.readString(), "RecordPart.json must not be null");
                    return new RecordPartImpl(json);
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class CdcSourceStateHook implements SerializerHook<CdcSourceP.State> {
        @Override
        public Class<CdcSourceP.State> getSerializationType() {
            return CdcSourceP.State.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<CdcSourceP.State>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.CDC_SOURCE_STATE;
                }

                @Override
                public void write(ObjectDataOutput out, CdcSourceP.State state) throws IOException {
                    out.writeObject(state.getPartitionsToOffset());

                    // workaround for https://github.com/hazelcast/hazelcast/issues/18129
                    // write the size hint, the list is concurrently modified, the actual number of items can be different
                    out.writeInt(state.getHistoryRecords().size());
                    for (byte[] r : state.getHistoryRecords()) {
                        assert r != null;
                        out.writeObject(r);
                    }
                    // terminator element
                    out.writeObject(null);
                }

                @Override
                public CdcSourceP.State read(ObjectDataInput in) throws IOException {
                    Map<Map<String, ?>, Map<String, ?>> partitionsToOffset = in.readObject();

                    // workaround for https://github.com/hazelcast/hazelcast/issues/18129
                    int sizeHint = in.readInt();
                    List<byte[]> historyRecords = new ArrayList<>(sizeHint);
                    // read the elements until a terminator is found
                    for (byte[] r; (r = in.readObject()) != null; ) {
                        historyRecords.add(r);
                    }

                    return new CdcSourceP.State(partitionsToOffset, new CopyOnWriteArrayList<>(historyRecords));
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

}
