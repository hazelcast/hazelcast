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

import com.hazelcast.vector.internal.impl.VectorTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ForkJoinPool;

import static com.hazelcast.config.vector.Metric.COSINE;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.toVectorFloat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VectorIndexTest extends AbstractVectorIndexTest {

    private AbstractVectorIndex vectorIndex;

    @BeforeEach
    void setup() {
        vectorIndex = new VectorIndexSingleKey(
                "name",
                COSINE,
                100,
                10,
                2,
                ForkJoinPool.commonPool()
        );
    }

    @Override
    public AbstractVectorIndex getVectorIndex() {
        return vectorIndex;
    }

    @Test
    void testPutTheSameKeyAndVector() {
        // prevent graph recreation during update
        getVectorIndex().put(data2, createVector(1, 1));

        getVectorIndex().put(data1, createVector(1, 1));
        getVectorIndex().put(data1, createVector(1, 1));

        assertThat(getVectorIndex().get(data1)).isEqualTo(toVectorFloat(1, 1));

        assertTrue(getVectorIndex().indexBuilder.getGraph().containsNode(0));
        assertTrue(getVectorIndex().indexBuilder.getGraph().containsNode(1));
        assertTrue(getVectorIndex().indexBuilder.getGraph().containsNode(2));
    }

    @Test
    void putTheSameVectorWithDifferentKeys() {
        vectorIndex.put(data1, createVector(2, 1));
        vectorIndex.put(data2, createVector(2, 1));

        assertTrue(vectorIndex.indexBuilder.getGraph().containsNode(0));
        assertTrue(vectorIndex.indexBuilder.getGraph().containsNode(1));

        var results = vectorIndex.search(createVector(1, 1), 3);
        var map = VectorTestUtils.toMap(results);
        assertThat(map.keySet()).containsExactlyInAnyOrderElementsOf(Set.of(data1, data2));
    }

    @Override
    protected long totalBytesUsedByEntries(int count) {
        return emptyIndexBytesUsed() + count * (getVectorIndex().vectorsSupplier.heapBytesUsedPerEntry
                + VectorIndexSingleKey.HEAP_BYTES_USED_PER_ENTRY)
                + getVectorIndex().indexBuilder.getGraph().ramBytesUsed()
                // if count is > 0, then subtract the fixed empty graph index cost we added in emptyIndexBytesUsed
                // since the graph index builder cost is already taken into account
                + (count == 0 ? 0 : -emptyGraphBytesUsed());
    }

    @Override
    protected long emptyIndexBytesUsed() {
        return AbstractVectorIndex.FIXED_HEAP_BYTES_USED
                + VectorIndexSingleKey.FIXED_HEAP_BYTES_USED
                + UpdatableVectorsSource.FIXED_HEAP_BYTES_USED
                // heap bytes used by empty graph index builder
                + emptyGraphBytesUsed();
    }
}
