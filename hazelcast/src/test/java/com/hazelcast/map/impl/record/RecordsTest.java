/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        Record record = mock(Record.class);
        when(record.getValue()).thenReturn(dataPayload);

        Records.getValueOrCachedValue(record, serializationService);
        verify(record, never()).setCachedValue(objectPayload);
    }

    @Test
    public void getValueOrCachedValue_whenRecordIsCachedDataRecordWithStats_thenCache() {
        String objectPayload = "foo";
        Data dataPayload = serializationService.toData(objectPayload);
        Record record = mock(CachedDataRecordWithStats.class);
        when(record.getValue()).thenReturn(dataPayload);

        Records.getValueOrCachedValue(record, serializationService);
        verify(record, times(1)).setCachedValue(objectPayload);
    }

    @Test
    public void getValueOrCachedValue_whenRecordIsCachedDataRecord_thenCache() {
        String objectPayload = "foo";
        Data dataPayload = serializationService.toData(objectPayload);
        Record record = mock(CachedDataRecord.class);
        when(record.getValue()).thenReturn(dataPayload);

        Records.getValueOrCachedValue(record, serializationService);
        verify(record, times(1)).setCachedValue(objectPayload);
    }
}
