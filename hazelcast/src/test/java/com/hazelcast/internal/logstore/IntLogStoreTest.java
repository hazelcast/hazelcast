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

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class IntLogStoreTest extends AbstractLogStoreTest {

    @Test
    public void testWriteInt() {
        IntLogStore store = createIntLogStore(new LogStoreConfig());

        long offset = store.eden.position;
        store.putInt(10);

        assertNotEquals(0, store.eden.address);
        assertEquals(offset + INT_SIZE_IN_BYTES, store.eden.position);
        assertEquals(10, UNSAFE.getInt(store.eden.address + offset));
    }

    @Test
    public void testWriteInt_overManySegments() {
        IntLogStore store = createIntLogStore(new LogStoreConfig().setSegmentSize(128));

        long[] sequences = new long[16 * 1024];
        int[] items = new int[sequences.length];
        Random r = new Random();
        for (int k = 0; k < sequences.length; k++) {
            int value = r.nextInt();
            items[k] = value;
            sequences[k] = store.putInt(items[k]);
            assertEquals(value, store.getInt(sequences[k]));
        }

        for (int k = 0; k < sequences.length; k++) {
            int expected = items[k];
            long sequence = sequences[k];
            int found = store.getInt(sequence);
            assertEquals(expected, found);
        }
    }

}
