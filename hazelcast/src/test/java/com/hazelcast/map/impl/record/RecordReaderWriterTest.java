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
package com.hazelcast.map.impl.record;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.nio.serialization.Data;
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

import static com.hazelcast.map.impl.record.RecordReaderWriter.DATA_RECORD_READER_WRITER;
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
    public void data_record_matching_reader_writer_id_is_data_record_reader_writer_id() {
        assertEquals(DATA_RECORD_READER_WRITER, new DataRecord().getMatchingRecordReaderWriter());
    }

    @Test
    public void data_record_with_stats_matching_reader_writer_id_is_data_record_with_stats_reader_writer_id() {
        assertEquals(DATA_RECORD_WITH_STATS_READER_WRITER, new DataRecordWithStats().getMatchingRecordReaderWriter());
    }

    @Test
    public void object_record_matching_reader_writer_id_is_data_record_reader_writer_id() {
        assertEquals(DATA_RECORD_READER_WRITER, new ObjectRecord().getMatchingRecordReaderWriter());
    }

    @Test
    public void object_record_with_stats_matching_reader_writer_id_is_data_record_with_stats_reader_writer_id() {
        assertEquals(DATA_RECORD_WITH_STATS_READER_WRITER, new ObjectRecordWithStats().getMatchingRecordReaderWriter());
    }

    @Test
    public void written_and_read_data_record_are_equal() throws IOException {
        Record<Data> writtenRecord = populateAndGetRecord(new DataRecord());
        Record<Data> readRecord = writeReadAndGet(writtenRecord, writtenRecord.getValue());

        assertEquals(writtenRecord, readRecord);
    }

    @Test
    public void written_and_read_data_record_with_stats_are_equal() throws IOException {
        Record<Data> writtenRecord = populateAndGetRecord(new DataRecordWithStats());
        Record<Data> readRecord = writeReadAndGet(writtenRecord, writtenRecord.getValue());

        assertEquals(writtenRecord, readRecord);
    }

    @Test
    public void written_and_read_object_record_are_equal() throws IOException {
        Record writtenRecord = populateAndGetRecord(new ObjectRecord());
        Data dataValue = ss.toData(writtenRecord.getValue());
        Record<Data> readRecord = writeReadAndGet(writtenRecord, dataValue);

        assertEquals(asDataRecord(writtenRecord, dataValue), readRecord);
    }

    @Test
    public void written_and_read_object_record_with_stats_are_equal() throws IOException {
        Record writtenRecord = populateAndGetRecord(new ObjectRecordWithStats());
        Data dataValue = ss.toData(writtenRecord.getValue());
        Record<Data> readRecord = writeReadAndGet(writtenRecord, dataValue);

        assertEquals(asDataRecordWithStats(writtenRecord, dataValue), readRecord);
    }

    private Record populateAndGetRecord(Record writtenRecord) {
        writtenRecord.setTtl(1);
        writtenRecord.setMaxIdle(2);
        writtenRecord.setVersion(3);
        writtenRecord.setLastUpdateTime(4);
        writtenRecord.setLastAccessTime(5);
        writtenRecord.setLastStoredTime(6);
        writtenRecord.setCreationTime(7);
        writtenRecord.setVersion(8);
        writtenRecord.setHits(9);
        writtenRecord.setKey(ss.toData(10));
        writtenRecord.setValue(ss.toData(11));
        return writtenRecord;
    }

    private Record writeReadAndGet(Record expectedRecord, Data dataValue) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectDataOutputStream out = new ObjectDataOutputStream(outputStream, ss);
        Records.writeRecord(out, expectedRecord, dataValue);
        ObjectDataInputStream in = new ObjectDataInputStream(new ByteArrayInputStream(outputStream.toByteArray()), ss);
        return Records.readRecord(in);
    }

    private static Record<Data> asDataRecord(Record fromRecord, Data value) {
        DataRecord toRecord = new DataRecord(value);
        toRecord.setKey(fromRecord.getKey());
        toRecord.setValue(value);
        return copyMetadata(fromRecord, toRecord);
    }

    private static Record<Data> asDataRecordWithStats(Record fromRecord, Data value) {
        DataRecordWithStats toRecord = new DataRecordWithStats(value);
        toRecord.setKey(fromRecord.getKey());
        toRecord.setValue(value);
        return copyMetadata(fromRecord, toRecord);
    }

    private static Record copyMetadata(Record fromRecord, Record toRecord) {
        toRecord.setHits(fromRecord.getHits());
        toRecord.setTtl(fromRecord.getTtl());
        toRecord.setMaxIdle(fromRecord.getMaxIdle());
        toRecord.setVersion(fromRecord.getVersion());
        toRecord.setMetadata(fromRecord.getMetadata());
        toRecord.setCreationTime(fromRecord.getCreationTime());
        toRecord.setExpirationTime(fromRecord.getExpirationTime());
        toRecord.setLastAccessTime(fromRecord.getLastAccessTime());
        toRecord.setLastStoredTime(fromRecord.getLastStoredTime());
        toRecord.setLastUpdateTime(fromRecord.getLastUpdateTime());
        return toRecord;
    }
}
