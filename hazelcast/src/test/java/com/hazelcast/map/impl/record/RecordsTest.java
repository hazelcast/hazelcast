/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
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
@Category({QuickTest.class, ParallelTest.class})
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
        Object firstDeserilizedValue = Records.getValueOrCachedValue(record, serializationService);
        assertEquals(objectPayload, firstDeserilizedValue);

        //we don't need serialization service for the 2nd call
        Object secondDeserilizedValue = Records.getValueOrCachedValue(record, null);
        assertSame(firstDeserilizedValue, secondDeserilizedValue);
    }

    @Test
    public void getValueOrCachedValue_whenRecordIsCachedDataRecord_thenCache() {
        String objectPayload = "foo";
        Data dataPayload = serializationService.toData(objectPayload);
        Record record = new CachedDataRecord(dataPayload);
        Object firstDeserilizedValue = Records.getValueOrCachedValue(record, serializationService);
        assertEquals(objectPayload, firstDeserilizedValue);

        //we don't need serialization service for the 2nd call
        Object secondDeserilizedValue = Records.getValueOrCachedValue(record, null);
        assertSame(firstDeserilizedValue, secondDeserilizedValue);
    }

    @Test
    public void givenCachedDataRecord_whenThreadIsInside_thenGetValueOrCachedValueReturnsTheThread() {
        //given
        CachedDataRecord record = new CachedDataRecord();

        //when
        SerializableThread objectPayload = new SerializableThread();
        Data dataPayload = serializationService.toData(objectPayload);
        record.setValue(dataPayload);

        //then
        Object cachedValue = Records.getValueOrCachedValue(record, serializationService);
        assertInstanceOf(SerializableThread.class, cachedValue);
    }

    @Test
    public void givenCachedDataRecordValueIsThread_whenCachedValueIsCreated_thenGetCachedValueReturnsTheThread() {
        //given
        SerializableThread objectPayload = new SerializableThread();
        Data dataPayload = serializationService.toData(objectPayload);
        CachedDataRecord record = new CachedDataRecord(dataPayload);

        //when
        Records.getValueOrCachedValue(record, serializationService);

        //then
        Object cachedValue = Records.getCachedValue(record);
        assertInstanceOf(SerializableThread.class, cachedValue);
    }


    @Test
    public void applyRecordInfo() throws Exception {
        long now = Clock.currentTimeMillis();
        RecordInfo recordInfo = newRecordInfo(now);

        Record record = new DataRecordWithStats();
        Records.applyRecordInfo(record, recordInfo);

        assertEquals(now, record.getCreationTime());
        assertEquals(now, record.getCreationTime());
        assertEquals(now, record.getLastAccessTime());
        assertEquals(now, record.getLastUpdateTime());
        assertEquals(123, record.getVersion());
        assertEquals(12, record.getHits());
        assertEquals(now, record.getStatistics().getExpirationTime());
        assertEquals(now, record.getStatistics().getLastStoredTime());
    }

    protected RecordInfo newRecordInfo(long now) {
        RecordInfo recordInfo = mock(RecordInfo.class);

        when(recordInfo.getCreationTime()).thenReturn(now);
        when(recordInfo.getLastAccessTime()).thenReturn(now);
        when(recordInfo.getLastUpdateTime()).thenReturn(now);
        when(recordInfo.getHits()).thenReturn(12L);
        when(recordInfo.getVersion()).thenReturn(123L);

        RecordStatistics statistics = mock(RecordStatistics.class);
        when(statistics.getExpirationTime()).thenReturn(now);
        when(statistics.getLastStoredTime()).thenReturn(now);
        when(recordInfo.getStatistics()).thenReturn(statistics);
        return recordInfo;
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
        assertEquals(now, recordInfo.getStatistics().getExpirationTime());
        assertEquals(now, recordInfo.getStatistics().getLastStoredTime());
    }

    protected Record newRecord(long now) {
        Record record = mock(Record.class, withSettings().extraInterfaces(RecordStatistics.class));

        when(record.getCreationTime()).thenReturn(now);
        when(record.getLastAccessTime()).thenReturn(now);
        when(record.getLastUpdateTime()).thenReturn(now);
        when(record.getHits()).thenReturn(12L);
        when(record.getVersion()).thenReturn(123L);
        when(record.getStatistics()).thenReturn(((RecordStatistics) record));
        when(((RecordStatistics) record).getExpirationTime()).thenReturn(now);
        when(((RecordStatistics) record).getLastStoredTime()).thenReturn(now);
        return record;
    }


    private static class SerializableThread extends Thread implements Serializable {

    }
}
