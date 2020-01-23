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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ByteLogStoreTest extends AbstractLogStoreTest {


    @Test
    public void testWriteByte() {
        ByteLogStore store = createByteLogStore(new LogStoreConfig());

        long offset = store.eden.position;
        store.putByte((byte) 10);

        assertNotEquals(0, store.eden.address);
        assertEquals(offset + 1, store.eden.position);
        assertEquals(10, UNSAFE.getByte(store.eden.address + offset));
    }

    @Test
    public void testManyBytes() {
        ByteLogStore store = createByteLogStore(new LogStoreConfig().setSegmentSize(128));

        long[] sequences = new long[16 * 1024];
        byte[] items = new byte[sequences.length];
        Random r = new Random();
        for (int k = 0; k < sequences.length; k++) {
            byte value = (byte) r.nextInt(100);
            items[k] = value;
            sequences[k] = store.putByte(items[k]);
            assertEquals(value, store.getByte(sequences[k]));
        }

        for (int k = 0; k < sequences.length; k++) {
            byte expected = items[k];
            long sequence = sequences[k];
            int found = store.getByte(sequence);
            assertEquals(expected, found);
        }
    }
}
