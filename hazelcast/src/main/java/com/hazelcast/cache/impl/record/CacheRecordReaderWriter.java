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

package com.hazelcast.cache.impl.record;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public enum CacheRecordReaderWriter {

    DATA(RecordTypeId.DATA) {
        @Override
        public CacheRecord read(ObjectDataInput in) throws IOException {
            CacheRecord<Data, Data> record = new CacheDataRecord();
            record.setCreationTime(in.readLong());
            record.setExpirationTime(in.readLong());
            record.setLastAccessTime(in.readLong());
            record.setHits(in.readInt());

            record.setValue(IOUtil.readData(in));
            record.setExpiryPolicy(IOUtil.readData(in));

            return record;
        }

        @Override
        public void write(CacheRecord record, ObjectDataOutput out) throws IOException {
            out.writeLong(record.getCreationTime());
            out.writeLong(record.getExpirationTime());
            out.writeLong(record.getLastAccessTime());
            out.writeInt((int) record.getHits());

            IOUtil.writeData(out, (Data) record.getValue());
            IOUtil.writeData(out, (Data) record.getExpiryPolicy());
        }
    },

    OBJECT(RecordTypeId.OBJECT) {
        @Override
        public CacheRecord read(ObjectDataInput in) throws IOException {
            CacheRecord record = new CacheObjectRecord();
            record.setCreationTime(in.readLong());
            record.setExpirationTime(in.readLong());
            record.setLastAccessTime(in.readLong());
            record.setHits(in.readInt());

            record.setValue(in.readObject());
            record.setExpiryPolicy(in.readObject());

            return record;
        }

        @Override
        public void write(CacheRecord record, ObjectDataOutput out) throws IOException {
            out.writeLong(record.getCreationTime());
            out.writeLong(record.getExpirationTime());
            out.writeLong(record.getLastAccessTime());
            out.writeInt((int) record.getHits());

            out.writeObject(record.getValue());
            out.writeObject(record.getExpiryPolicy());
        }
    };

    private byte recordTypeId;

    CacheRecordReaderWriter(byte recordTypeId) {
        this.recordTypeId = recordTypeId;
    }

    public static void writeCacheRecord(CacheRecord cacheRecord, ObjectDataOutput out) throws IOException {
        CacheRecordReaderWriter readerWriter = getByRecord(cacheRecord);
        out.writeByte(readerWriter.recordTypeId);
        readerWriter.write(cacheRecord, out);
    }

    public static CacheRecord readCacheRecord(ObjectDataInput in) throws IOException {
        byte recordTypeId = in.readByte();
        CacheRecordReaderWriter readerWriter = getByRecordTypeId(recordTypeId);
        return readerWriter.read(in);
    }

    public static CacheRecordReaderWriter getByRecordTypeId(int id) {
        switch (id) {
            case RecordTypeId.DATA:
                return DATA;
            case RecordTypeId.OBJECT:
                return OBJECT;
            default:
                throw new IllegalArgumentException("Not known CacheRecordReaderWriter type-id: " + id);
        }
    }

    private static CacheRecordReaderWriter getByRecord(CacheRecord cacheRecord) {
        if (cacheRecord instanceof CacheObjectRecord) {
            return OBJECT;
        }

        return DATA;
    }

    private static class RecordTypeId {
        private static final byte DATA = 1;
        private static final byte OBJECT = 2;

    }

    abstract void write(CacheRecord record, ObjectDataOutput out) throws IOException;

    abstract CacheRecord read(ObjectDataInput in) throws IOException;
}
