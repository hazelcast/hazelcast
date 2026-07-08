/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.vector.internal.impl.VectorTestUtils.randomVectorFloat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class UpdatableVectorsSourceTest {

    private UpdatableVectorsSource source;

    @BeforeEach
    void setup() {
        source = new UpdatableVectorsSource(32);
    }

    @Test
    void testHeapBytesUsed_whenEmpty() {
        assertEquals(UpdatableVectorsSource.FIXED_HEAP_BYTES_USED, source.heapBytesUsed());
    }

    @Test
    void testHeapBytesUsed_whenEntriesAdded() {
        int numEntries = ThreadLocalRandom.current().nextInt(100);
        for (int i = 0; i < numEntries; i++) {
            source.add(i, randomVectorFloat(32));
        }
        long expected = UpdatableVectorsSource.FIXED_HEAP_BYTES_USED + numEntries * source.heapBytesUsedPerEntry;
        assertEquals(expected, source.heapBytesUsed());
    }

    @Test
    void testHeapBytesUsed_whenEntriesAddedThenSomeRemoved() {
        int numEntries = ThreadLocalRandom.current().nextInt(100);
        for (int i = 0; i < numEntries; i++) {
            source.add(i, randomVectorFloat(32));
        }
        for (int i = 0; i < numEntries / 2; i++) {
            source.remove(i);
        }
        long expected = UpdatableVectorsSource.FIXED_HEAP_BYTES_USED
                + (numEntries - numEntries / 2) * source.heapBytesUsedPerEntry;
        assertEquals(expected, source.heapBytesUsed());
    }

    @Test
    void testHeapBytesUsed_whenEntriesAddedThenAllRemoved() {
        int numEntries = ThreadLocalRandom.current().nextInt(100);
        for (int i = 0; i < numEntries; i++) {
            source.add(i, randomVectorFloat(32));
        }
        for (int i = 0; i < numEntries; i++) {
            source.remove(i);
        }
        assertEquals(UpdatableVectorsSource.FIXED_HEAP_BYTES_USED, source.heapBytesUsed());
    }
}
