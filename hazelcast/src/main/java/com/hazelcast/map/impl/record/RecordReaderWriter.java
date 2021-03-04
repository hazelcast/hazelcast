/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.record;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.internal.nio.IOUtil.readData;
import static com.hazelcast.internal.nio.IOUtil.writeData;

/**
 * Used when reading and writing records
 * for backup and replication operations
 */
public enum RecordReaderWriter {
    // RU_COMPAT_4_1
    // Remove enum DATA_RECORD_READER_WRITER in 4.3
    DATA_RECORD_READER_WRITER(TypeId.DATA_RECORD_TYPE_ID) {
        @Override
        void writeRecord(ObjectDataOutput out, Record record, Data dataValue,
                         ExpiryMetadata expiryMetadata) throws IOException {
            writeData(out, dataValue);
            out.writeInt(expiryMetadata.getRawTtl());
            out.writeInt(expiryMetadata.getRawMaxIdle());
            out.writeInt(record.getRawCreationTime());
            out.writeInt(record.getRawLastAccessTime());
            out.writeInt(record.getRawLastUpdateTime());
            out.writeInt(record.getHits());
            out.writeLong(record.getVersion());
        }

        @Override
        public Record readRecord(ObjectDataInput in,
                                 ExpiryMetadata expiryMetadata) throws IOException {
            Record record = new DataRecordWithStats();
            record.setValue(readData(in));
            expiryMetadata.setRawTtl(in.readInt());
            expiryMetadata.setRawMaxIdle(in.readInt());
            record.setRawCreationTime(in.readInt());
            record.setRawLastAccessTime(in.readInt());
            record.setRawLastUpdateTime(in.readInt());
            record.setHits(in.readInt());
            record.setVersion(longToIntVersion(in.readLong()));

            return record;
        }
    },

    DATA_RECORD_WITH_STATS_READER_WRITER(TypeId.DATA_RECORD_WITH_STATS_TYPE_ID) {
        @Override
        void writeRecord(ObjectDataOutput out, Record record, Data dataValue,
                         ExpiryMetadata expiryMetadata) throws IOException {
            writeData(out, dataValue);
            out.writeInt(expiryMetadata.getRawTtl());
            out.writeInt(expiryMetadata.getRawMaxIdle());
            out.writeInt(record.getRawCreationTime());
            out.writeInt(record.getRawLastAccessTime());
            out.writeInt(record.getRawLastUpdateTime());
            out.writeInt(record.getHits());
            out.writeLong(record.getVersion());
            out.writeInt(record.getRawLastStoredTime());
            out.writeInt(expiryMetadata.getRawExpirationTime());
        }

        @Override
        public Record readRecord(ObjectDataInput in,
                                 ExpiryMetadata expiryMetadata) throws IOException {
            Record record = new DataRecordWithStats();
            record.setValue(readData(in));
            expiryMetadata.setRawTtl(in.readInt());
            expiryMetadata.setRawMaxIdle(in.readInt());
            record.setRawCreationTime(in.readInt());
            record.setRawLastAccessTime(in.readInt());
            record.setRawLastUpdateTime(in.readInt());
            record.setHits(in.readInt());
            record.setVersion(longToIntVersion(in.readLong()));
            record.setRawLastStoredTime(in.readInt());
            expiryMetadata.setRawExpirationTime(in.readInt());

            return record;
        }
    },

    SIMPLE_DATA_RECORD_READER_WRITER(TypeId.SIMPLE_DATA_RECORD_TYPE_ID) {
        @Override
        void writeRecord(ObjectDataOutput out, Record record, Data dataValue,
                         ExpiryMetadata expiryMetadata) throws IOException {
            writeData(out, dataValue);
            out.writeInt(record.getVersion());
            out.writeInt(expiryMetadata.getRawTtl());
            out.writeInt(expiryMetadata.getRawMaxIdle());
            out.writeInt(expiryMetadata.getRawExpirationTime());
        }

        @Override
        public Record readRecord(ObjectDataInput in,
                                 ExpiryMetadata expiryMetadata) throws IOException {
            Record record = new SimpleRecord();
            record.setValue(readData(in));
            record.setVersion(in.readInt());
            expiryMetadata.setRawTtl(in.readInt());
            expiryMetadata.setRawMaxIdle(in.readInt());
            expiryMetadata.setRawExpirationTime(in.readInt());

            return record;
        }
    },

    SIMPLE_DATA_RECORD_WITH_LRU_EVICTION_READER_WRITER(TypeId.SIMPLE_DATA_RECORD_WITH_LRU_EVICTION_TYPE_ID) {
        @Override
        void writeRecord(ObjectDataOutput out, Record record, Data dataValue,
                         ExpiryMetadata expiryMetadata) throws IOException {
            writeData(out, dataValue);
            out.writeInt(record.getVersion());
            out.writeInt(record.getRawLastAccessTime());
            out.writeInt(expiryMetadata.getRawTtl());
            out.writeInt(expiryMetadata.getRawMaxIdle());
            out.writeInt(expiryMetadata.getRawExpirationTime());
        }

        @Override
        public Record readRecord(ObjectDataInput in,
                                 ExpiryMetadata expiryMetadata) throws IOException {
            Record record = new SimpleRecordWithLRUEviction();
            record.setValue(readData(in));
            record.setVersion(in.readInt());
            record.setRawLastAccessTime(in.readInt());
            expiryMetadata.setRawTtl(in.readInt());
            expiryMetadata.setRawMaxIdle(in.readInt());
            expiryMetadata.setRawExpirationTime(in.readInt());

            return record;
        }
    },

    SIMPLE_DATA_RECORD_WITH_LFU_EVICTION_READER_WRITER(TypeId.SIMPLE_DATA_RECORD_WITH_LFU_EVICTION_TYPE_ID) {
        @Override
        void writeRecord(ObjectDataOutput out, Record record, Data dataValue,
                         ExpiryMetadata expiryMetadata) throws IOException {
            writeData(out, dataValue);
            out.writeInt(record.getVersion());
            out.writeInt(record.getHits());
            out.writeInt(expiryMetadata.getRawTtl());
            out.writeInt(expiryMetadata.getRawMaxIdle());
            out.writeInt(expiryMetadata.getRawExpirationTime());
        }

        @Override
        public Record readRecord(ObjectDataInput in,
                                 ExpiryMetadata expiryMetadata) throws IOException {
            Record record = new SimpleRecordWithLFUEviction();
            record.setValue(readData(in));
            record.setVersion(in.readInt());
            record.setHits(in.readInt());
            expiryMetadata.setRawTtl(in.readInt());
            expiryMetadata.setRawMaxIdle(in.readInt());
            expiryMetadata.setRawExpirationTime(in.readInt());

            return record;
        }
    };

    private byte id;

    RecordReaderWriter(byte id) {
        this.id = id;
    }

    public byte getId() {
        return id;
    }

    private static class TypeId {
        private static final byte DATA_RECORD_TYPE_ID = 1;
        private static final byte DATA_RECORD_WITH_STATS_TYPE_ID = 2;
        private static final byte SIMPLE_DATA_RECORD_TYPE_ID = 3;
        private static final byte SIMPLE_DATA_RECORD_WITH_LRU_EVICTION_TYPE_ID = 4;
        private static final byte SIMPLE_DATA_RECORD_WITH_LFU_EVICTION_TYPE_ID = 5;
    }

    public static RecordReaderWriter getById(int id) {
        switch (id) {
            case TypeId.DATA_RECORD_TYPE_ID:
                return DATA_RECORD_READER_WRITER;
            case TypeId.DATA_RECORD_WITH_STATS_TYPE_ID:
                return DATA_RECORD_WITH_STATS_READER_WRITER;
            case TypeId.SIMPLE_DATA_RECORD_TYPE_ID:
                return SIMPLE_DATA_RECORD_READER_WRITER;
            case TypeId.SIMPLE_DATA_RECORD_WITH_LRU_EVICTION_TYPE_ID:
                return SIMPLE_DATA_RECORD_WITH_LRU_EVICTION_READER_WRITER;
            case TypeId.SIMPLE_DATA_RECORD_WITH_LFU_EVICTION_TYPE_ID:
                return SIMPLE_DATA_RECORD_WITH_LFU_EVICTION_READER_WRITER;
            default:
                throw new IllegalArgumentException("Not known RecordReaderWriter type-id: " + id);
        }
    }

    private static int longToIntVersion(long version) {
        return version >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) version;
    }

    abstract void writeRecord(ObjectDataOutput out,
                              Record record, Data dataValue, ExpiryMetadata expiryMetadata) throws IOException;

    public abstract Record readRecord(ObjectDataInput in, ExpiryMetadata expiryMetadata) throws IOException;
}
