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

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RecordsTest extends HazelcastTestSupport {

    private SerializationService serializationService;

    @Before
    public void setUp() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void getValueOrCachedValue_whenRecordIsNotCachable_thenDoNotCache() {
        String objectPayload = "foo";
        Data dataPayload = serializationService.toData(objectPayload);
        Record record = new DataRecord(dataPayload);
        Object value = Records.getValueOrCachedValue(record, null);
        assertSame(dataPayload, value);
    }

    @Test
    public void getValueOrCachedValue_whenRecordIsCachedDataRecordWithStats_thenCache() {
        String objectPayload = "foo";
        Data dataPayload = serializationService.toData(objectPayload);
        Record record = new CachedDataRecordWithStats(dataPayload);
        Object firstDeserializedValue = Records.getValueOrCachedValue(record, serializationService);
        assertEquals(objectPayload, firstDeserializedValue);

        // we don't need serialization service for the 2nd call
        Object secondDeserializedValue = Records.getValueOrCachedValue(record, null);
        assertSame(firstDeserializedValue, secondDeserializedValue);
    }

    @Test
    public void getValueOrCachedValue_whenRecordIsCachedDataRecord_thenCache() {
        String objectPayload = "foo";
        Data dataPayload = serializationService.toData(objectPayload);
        Record record = new CachedDataRecord(dataPayload);
        Object firstDeserializedValue = Records.getValueOrCachedValue(record, serializationService);
        assertEquals(objectPayload, firstDeserializedValue);

        // we don't need serialization service for the 2nd call
        Object secondDeserializedValue = Records.getValueOrCachedValue(record, null);
        assertSame(firstDeserializedValue, secondDeserializedValue);
    }

    @Test
    public void givenCachedDataRecord_whenThreadIsInside_thenGetValueOrCachedValueReturnsTheThread() {
        // given
        CachedDataRecord record = new CachedDataRecord();

        // when
        SerializableThread objectPayload = new SerializableThread();
        Data dataPayload = serializationService.toData(objectPayload);
        record.setValue(dataPayload);

        // then
        Object cachedValue = Records.getValueOrCachedValue(record, serializationService);
        assertInstanceOf(SerializableThread.class, cachedValue);
    }

    @Test
    public void givenCachedDataRecordValueIsThread_whenCachedValueIsCreated_thenGetCachedValueReturnsTheThread() {
        // given
        SerializableThread objectPayload = new SerializableThread();
        Data dataPayload = serializationService.toData(objectPayload);
        CachedDataRecord record = new CachedDataRecord(dataPayload);

        // when
        Records.getValueOrCachedValue(record, serializationService);

        // then
        Object cachedValue = Records.getCachedValue(record);
        assertInstanceOf(SerializableThread.class, cachedValue);
    }

    @Test
    public void applyRecordInfo() {
        // Shared key&value by referenceRecord and recordInfo-applied-recordInfoAppliedRecord
        Data key = new HeapData();
        Data value = new HeapData();

        // Create recordInfo from a reference record
        Record referenceRecord = new DataRecordWithStats();
        referenceRecord.setKey(key);
        referenceRecord.setValue(value);
        referenceRecord.setHits(123);
        referenceRecord.setVersion(12);
        RecordInfo recordInfo = toRecordInfo(referenceRecord);

        // Apply created recordInfo to recordInfoAppliedRecord
        Record recordInfoAppliedRecord = new DataRecordWithStats();
        recordInfoAppliedRecord.setKey(key);
        recordInfoAppliedRecord.setValue(value);

        Records.applyRecordInfo(recordInfoAppliedRecord, recordInfo);

        // Check recordInfo applied correctly to recordInfoAppliedRecord
        assertEquals(referenceRecord, recordInfoAppliedRecord);
    }

    @Test
    public void buildRecordInfo() throws Exception {
        long now = Clock.currentTimeMillis();

        Record record = newRecord(now);

        RecordInfo recordInfo = Records.buildRecordInfo(record);

        assertEquals(now, recordInfo.getCreationTime());
        assertEquals(now, recordInfo.getLastAccessTime());
        assertEquals(now, recordInfo.getLastUpdateTime());
        assertEquals(12, recordInfo.getHits());
        assertEquals(123, recordInfo.getVersion());
        assertEquals(now, recordInfo.getExpirationTime());
        assertEquals(now, recordInfo.getLastStoredTime());
    }

    private static RecordInfo toRecordInfo(Record record) {
        RecordInfo recordInfo = mock(RecordInfo.class);

        when(recordInfo.getCreationTime()).thenReturn(record.getCreationTime());
        when(recordInfo.getLastAccessTime()).thenReturn(record.getLastAccessTime());
        when(recordInfo.getLastUpdateTime()).thenReturn(record.getLastUpdateTime());
        when(recordInfo.getHits()).thenReturn(record.getHits());
        when(recordInfo.getVersion()).thenReturn(record.getVersion());
        when(recordInfo.getExpirationTime()).thenReturn(record.getExpirationTime());
        when(recordInfo.getLastStoredTime()).thenReturn(record.getLastStoredTime());

        return recordInfo;
    }

    private static Record newRecord(long now) {
        Record record = mock(Record.class, withSettings());

        when(record.getCreationTime()).thenReturn(now);
        when(record.getLastAccessTime()).thenReturn(now);
        when(record.getLastUpdateTime()).thenReturn(now);
        when(record.getHits()).thenReturn(12L);
        when(record.getVersion()).thenReturn(123L);
        when(record.getExpirationTime()).thenReturn(now);
        when(record.getLastStoredTime()).thenReturn(now);
        return record;
    }

    private static class SerializableThread extends Thread implements Serializable {
    }
}
