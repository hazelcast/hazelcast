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

package com.hazelcast.internal.logstore;

import org.junit.Test;

import java.util.Random;

import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class LongLogStoreTest extends AbstractLogStoreTest {

    @Test
    public void testPutLong() {
        LongLogStore store = createLongLogStore(new LogStoreConfig());

        long offset = store.eden.position;
        store.putLong(10);

        assertNotEquals(0, store.eden.address);
        assertEquals(offset + LONG_SIZE_IN_BYTES, store.eden.position);
        assertEquals(10, UNSAFE.getLong(store.eden.address + offset));
    }

    @Test
    public void testGetLong_overManySegments() {
        LongLogStore store = createLongLogStore(new LogStoreConfig().setSegmentSize(128));

        long[] sequences = new long[16 * 1024];
        long[] items = new long[sequences.length];
        Random r = new Random();
        for (int k = 0; k < sequences.length; k++) {
            long value = r.nextLong();
            items[k] = value;
            sequences[k] = store.putLong(items[k]);
            assertEquals(value, store.getLong(sequences[k]));
        }

        for (int k = 0; k < sequences.length; k++) {
            long expected = items[k];
            long sequence = sequences[k];
            long found = store.getLong(sequence);
            assertEquals(expected, found);
        }
    }

    @Test
    public void testManyBytes_withMaxSegmentCount() {
        LogStoreConfig config = new LogStoreConfig().setSegmentSize(128).setMaxSegmentCount(5);
        LongLogStore store = createLongLogStore(config);

        for (int k = 0; k < 1000; k++) {
            long sequence = store.putLong(k);
            assertEquals(k * LONG_SIZE_IN_BYTES, sequence);
            assertTrue("segmentCount:" + store.segmentCount, store.segmentCount() <= 5);
        }

        assertEquals(config.getMaxSegmentCount(), store.segmentCount());
    }

}
