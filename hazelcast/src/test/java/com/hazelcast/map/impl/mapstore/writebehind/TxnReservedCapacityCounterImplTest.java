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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TxnReservedCapacityCounterImplTest {
    HazelcastProperties hazelcastProperties = new HazelcastProperties(new Properties());
    NodeWideUsedCapacityCounter nodeWideUsedCapacityCounter
            = new NodeWideUsedCapacityCounter(hazelcastProperties);
    TxnReservedCapacityCounter counter = new TxnReservedCapacityCounterImpl(nodeWideUsedCapacityCounter);

    @Test
    public void increment() {
        UUID txnId = UuidUtil.newSecureUUID();
        for (int i = 0; i < 11; i++) {
            counter.increment(txnId, false);
        }

        Map<UUID, Long> countPerTxnId = counter.getReservedCapacityCountPerTxnId();
        long count = countPerTxnId.get(txnId);

        assertEquals(11L, count);
        assertEquals(11L, nodeWideUsedCapacityCounter.currentValue());
    }

    @Test
    public void decrement() {
        UUID txnId = UuidUtil.newSecureUUID();
        for (int i = 0; i < 11; i++) {
            counter.increment(txnId, false);
        }

        for (int i = 0; i < 11; i++) {
            counter.decrement(txnId);
        }

        Map<UUID, Long> countPerTxnId = counter.getReservedCapacityCountPerTxnId();
        assertNull(countPerTxnId.get(txnId));
        assertEquals(0L, nodeWideUsedCapacityCounter.currentValue());
    }

    @Test
    public void decrementOnlyReserved() {
        UUID txnId = UuidUtil.newSecureUUID();
        for (int i = 0; i < 11; i++) {
            counter.increment(txnId, false);
        }

        for (int i = 0; i < 11; i++) {
            counter.decrementOnlyReserved(txnId);
        }

        Map<UUID, Long> countPerTxnId = counter.getReservedCapacityCountPerTxnId();
        assertNull(countPerTxnId.get(txnId));
        assertEquals(11L, nodeWideUsedCapacityCounter.currentValue());
    }

    @Test
    public void putAll() {
        UUID txnId1 = UuidUtil.newSecureUUID();
        UUID txnId2 = UuidUtil.newSecureUUID();
        UUID txnId3 = UuidUtil.newSecureUUID();

        Map<UUID, Long> givenCountPerTxnId = new HashMap<>();
        givenCountPerTxnId.put(txnId1, 11L);
        givenCountPerTxnId.put(txnId2, 12L);
        givenCountPerTxnId.put(txnId3, 13L);

        // batch increment with a map of counts.
        counter.putAll(givenCountPerTxnId);

        Map<UUID, Long> countPerTxnId = counter.getReservedCapacityCountPerTxnId();
        long count1 = countPerTxnId.get(txnId1);
        long count2 = countPerTxnId.get(txnId2);
        long count3 = countPerTxnId.get(txnId3);

        assertEquals(11L, count1);
        assertEquals(12L, count2);
        assertEquals(13L, count3);
        assertEquals(11L + 12L + 13L, nodeWideUsedCapacityCounter.currentValue());
    }

    @Test
    public void releaseAllReservations() {
        UUID txnId = UuidUtil.newSecureUUID();
        for (int i = 0; i < 11; i++) {
            counter.increment(txnId, false);
        }
        counter.releaseAllReservations();

        Map<UUID, Long> countPerTxnId = counter.getReservedCapacityCountPerTxnId();
        assertNull(countPerTxnId.get(txnId));
        assertEquals(0L, nodeWideUsedCapacityCounter.currentValue());
    }
}
