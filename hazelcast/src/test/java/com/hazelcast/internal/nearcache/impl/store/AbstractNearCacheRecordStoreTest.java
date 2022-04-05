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

package com.hazelcast.internal.nearcache.impl.store;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.nearcache.NearCache.UpdateSemantic.READ_UPDATE;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.READ_PERMITTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractNearCacheRecordStoreTest {

    private static final int KEY = 23;
    private static final int VALUE1 = 42;
    private static final int VALUE2 = 2342;

    private SerializationService serializationService;
    private AbstractNearCacheRecordStore store;

    @Before
    public void setUp() {
        NearCacheConfig config = new NearCacheConfig()
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setSerializeKeys(false)
                .setInvalidateOnChange(true);

        serializationService = new DefaultSerializationServiceBuilder().build();

        store = new NearCacheObjectRecordStore("name", config, serializationService, getClass().getClassLoader());
        store.initialize();
    }

    @After
    public void tearDown() {
        store.destroy();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRecordCreation_withReservation() {
        Data keyData = serializationService.toData(KEY);

        long reservationId1 = store.tryReserveForUpdate(KEY, keyData, READ_UPDATE);
        long reservationId2 = store.tryReserveForUpdate(KEY, keyData, READ_UPDATE);

        // only one reservation ID is given for the same key
        assertNotEquals(NOT_RESERVED, reservationId1);
        assertEquals(NOT_RESERVED, reservationId2);
        assertRecordState(reservationId1);

        // cannot publish the value with the wrong reservation ID
        assertNull(store.tryPublishReserved(KEY, VALUE2, reservationId2, true));
        assertRecordState(reservationId1);

        // can publish the value with the correct reservation ID
        assertEquals(VALUE1, store.tryPublishReserved(KEY, VALUE1, reservationId1, true));
        assertRecordState(READ_PERMITTED);

        // cannot change a published value with the wrong reservation ID
        assertEquals(VALUE1, store.tryPublishReserved(KEY, VALUE2, reservationId2, true));
        assertRecordState(READ_PERMITTED);

        // cannot change a published value with the correct reservation ID
        assertEquals(VALUE1, store.tryPublishReserved(KEY, VALUE2, reservationId1, true));
        assertRecordState(READ_PERMITTED);

        // only a single record has been created
        assertEquals(1, store.records.size());
        assertEquals(1, store.getNearCacheStats().getOwnedEntryCount());
    }

    @SuppressWarnings("unchecked")
    private void assertRecordState(long recordState) {
        assertEquals(recordState, store.getRecord(KEY).getReservationId());
    }
}
