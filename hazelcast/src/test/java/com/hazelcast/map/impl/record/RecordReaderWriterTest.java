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
package com.hazelcast.map.impl.record;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadataImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.hazelcast.map.impl.record.RecordReaderWriter.DATA_RECORD_WITH_STATS_READER_WRITER;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RecordReaderWriterTest {

    InternalSerializationService ss;

    @Before
    public void setUp() throws Exception {
        DefaultSerializationServiceBuilder ssBuilder = new DefaultSerializationServiceBuilder();
        ss = ssBuilder.setVersion(InternalSerializationService.VERSION_1).build();
    }

    @Test
    public void data_record_with_stats_matching_reader_writer_id_is_data_record_with_stats_reader_writer_id() {
        assertEquals(DATA_RECORD_WITH_STATS_READER_WRITER, new DataRecordWithStats().getMatchingRecordReaderWriter());
    }

    @Test
    public void object_record_with_stats_matching_reader_writer_id_is_data_record_with_stats_reader_writer_id() {
        assertEquals(DATA_RECORD_WITH_STATS_READER_WRITER, new ObjectRecordWithStats().getMatchingRecordReaderWriter());
    }

    private ExpiryMetadata newExpiryMetadata() {
        return new ExpiryMetadataImpl();
    }

    @Test
    public void written_and_read_data_record_with_stats_are_equal() throws IOException {
        ExpiryMetadata expiryMetadata = newExpiryMetadata();
        Record<Data> writtenRecord = populateAndGetRecord(new DataRecordWithStats(), expiryMetadata);
        Record<Data> readRecord = writeReadAndGet(writtenRecord, writtenRecord.getValue(), expiryMetadata);

        assertEquals(writtenRecord, readRecord);
    }

    @Test
    public void written_and_read_object_record_with_stats_are_equal() throws IOException {
        ExpiryMetadata expiryMetadata = newExpiryMetadata();
        Record writtenRecord = populateAndGetRecord(new ObjectRecordWithStats(), expiryMetadata);
        Data dataValue = ss.toData(writtenRecord.getValue());
        Record<Data> readRecord = writeReadAndGet(writtenRecord, dataValue, expiryMetadata);

        assertEquals(asDataRecordWithStats(writtenRecord, dataValue), readRecord);
    }

    private Record populateAndGetRecord(Record writtenRecord, ExpiryMetadata expiryMetadata) {
        writtenRecord.setVersion(3);
        writtenRecord.setLastUpdateTime(4);
        writtenRecord.setLastAccessTime(5);
        writtenRecord.setLastStoredTime(6);
        writtenRecord.setCreationTime(8);
        writtenRecord.setVersion(9);
        writtenRecord.setHits(10);
        writtenRecord.setValue(ss.toData(11));

        expiryMetadata.setTtl(1);
        expiryMetadata.setMaxIdle(2);
        expiryMetadata.setExpirationTime(7);
        return writtenRecord;
    }

    private Record writeReadAndGet(Record expectedRecord, Data dataValue, ExpiryMetadata expiryMetadata) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectDataOutputStream out = new ObjectDataOutputStream(outputStream, ss);
        Records.writeRecord(out, expectedRecord, dataValue);
        ObjectDataInputStream in = new ObjectDataInputStream(new ByteArrayInputStream(outputStream.toByteArray()), ss);
        return Records.readRecord(in);
    }

    private static Record<Data> asDataRecordWithStats(Record fromRecord, Data value) {
        DataRecordWithStats toRecord = new DataRecordWithStats(value);
        toRecord.setValue(value);
        return copyMetadata(fromRecord, toRecord);
    }

    private static Record copyMetadata(Record fromRecord, Record toRecord) {
        toRecord.setHits(fromRecord.getHits());
        toRecord.setVersion(fromRecord.getVersion());
        toRecord.setCreationTime(fromRecord.getCreationTime());
        toRecord.setLastAccessTime(fromRecord.getLastAccessTime());
        toRecord.setLastStoredTime(fromRecord.getLastStoredTime());
        toRecord.setLastUpdateTime(fromRecord.getLastUpdateTime());
        return toRecord;
    }
}
