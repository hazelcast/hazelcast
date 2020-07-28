/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Hazelcast serializer hooks for data objects involved in processing
 * change data capture streams.
 */
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
                    out.writeLong(record.sequenceSource());
                    out.writeLong(record.sequenceValue());
                    out.writeUTF(record.getKeyJson());
                    out.writeUTF(record.getValueJson());
                }

                @Override
                public ChangeRecordImpl read(ObjectDataInput in) throws IOException {
                    long sequenceSource = in.readLong();
                    long sequenceValue = in.readLong();
                    String keyJson = in.readUTF();
                    String valueJson = in.readUTF();
                    return new ChangeRecordImpl(sequenceSource, sequenceValue, keyJson, valueJson);
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
                    out.writeUTF(part.toJson());
                }

                @Override
                public RecordPartImpl read(ObjectDataInput in) throws IOException {
                    String json = in.readUTF();
                    return new RecordPartImpl(json);
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class CdcSourceStateHook implements SerializerHook<CdcSource.State> {
        @Override
        public Class<CdcSource.State> getSerializationType() {
            return CdcSource.State.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<CdcSource.State>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.CDC_SOURCE_STATE;
                }

                @Override
                public void write(ObjectDataOutput out, CdcSource.State state) throws IOException {
                    out.writeObject(state.getPartitionsToOffset());
                    out.writeObject(state.getHistoryRecords());
                }

                @Override
                public CdcSource.State read(ObjectDataInput in) throws IOException {
                    Map<Map<String, ?>, Map<String, ?>> partitionsToOffset = in.readObject();
                    CopyOnWriteArrayList<byte[]> historyRecords = in.readObject();
                    return new CdcSource.State(partitionsToOffset, historyRecords);
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

}
