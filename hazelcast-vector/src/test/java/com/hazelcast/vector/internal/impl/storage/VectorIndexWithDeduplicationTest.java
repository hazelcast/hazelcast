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

import com.hazelcast.internal.serialization.Data;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;

import static com.hazelcast.config.vector.Metric.COSINE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VectorIndexWithDeduplicationTest extends AbstractVectorIndexTest {

    private AbstractVectorIndex vectorIndex;

    @BeforeEach
    void setup() {
        vectorIndex = new VectorIndexMultipleKeys(
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
    void putTheSameVectorWithDifferentKeys() {
        vectorIndex.put(data1, createVector(2, 1));
        vectorIndex.put(data2, createVector(2, 1));

        assertGraphContainsExactNodesId(Collections.singleton(0));

        var results = vectorIndex.search(createVector(1, 1), 3);
        Map<Data, Integer> actualData = toKeyNodeIdMap(results);
        Map<Data, Integer> expected = Map.of(
                data1, 0,
                data2, 0
        );
        assertThat(actualData).isEqualTo(expected);
    }

    @Test
    void testPutTheSameKeyAndVector() {
        getVectorIndex().put(data1, createVector(1, 1));
        getVectorIndex().put(data1, createVector(1, 1));

        assertThat(getVectorIndex().get(data1)).isEqualTo(createVector(1, 1));

        assertTrue(getVectorIndex().indexBuilder.getGraph().containsNode(0));
        assertFalse(getVectorIndex().indexBuilder.getGraph().containsNode(1));
    }

    /*
    Given:
        key1 -> vector1 -> id1
        key2 -> vector2 -> id2
    When:
        add key1 -> vector2
    Then:
        vector1 and id1 deleted after cleanup
        key1 -> vector2 -> id2
        key2 -> vector2 -> id2
    */
    @Test
    void remapAndDeletePrevious() {
        vectorIndex.put(data1, createVector(1, 0));
        vectorIndex.put(data2, createVector(0, 1));

        assertGraphContainsExactNodesId(Set.of(0, 1));

        vectorIndex.put(data1, createVector(0, 1));

        var results = vectorIndex.search(createVector(0, 2), 3);
        Map<Data, Integer> actualData = toKeyNodeIdMap(results);
        Map<Data, Integer> expectedData = Map.of(
                data1, 1,
                data2, 1
        );
        assertThat(actualData).isEqualTo(expectedData);

        vectorIndex.indexBuilder.cleanup();
        assertGraphContainsExactNodesId(Collections.singleton(1));
    }


    /*
    Given:
        key1 -> vector1 -> id1
        key2 -> vector1 -> id1
        key3 -> vector2 -> id2
    When:
        add key1 -> vector2
    Then:
        key2 -> vector1 -> id1
        key1 -> vector2 -> id2
        key3 -> vector2 -> id2
    */
    @Test
    void remappingKey() {
        var vector1 = createVector(1, 0);
        var vector2 = createVector(0, 1);
        vectorIndex.put(data1, vector1);
        vectorIndex.put(data2, vector1);
        vectorIndex.put(data3, vector2);

        vectorIndex.put(data1, vector2);

        var results = vectorIndex.search(createVector(0, 2), 4);
        Map<Data, Integer> actualVectorKeys = toKeyNodeIdMap(results);
        Map<Data, Integer> expectedVectorKeys = Map.of(
                data2, 0,
                data1, 1,
                data3, 1
        );
        assertThat(actualVectorKeys).isEqualTo(expectedVectorKeys);
    }

    /*
    Given:
        key1 -> vector1 -> id1
        key2 -> vector1 -> id1
    When:
        delete key1
    Then:
        key2 -> vector1 -> id1
    */
    @Test
    void twoVectorReferenceToOneKey_deleteKey_then_vectorExists() {
        var vector = createVector(1, 0);
        vectorIndex.put(data1, vector);
        vectorIndex.put(data2, vector);

        vectorIndex.delete(data1);

        var results = vectorIndex.search(createVector(0, 2), 2);
        Map<Data, Integer> actualVectorKeys = toKeyNodeIdMap(results);
        Map<Data, Integer> expectedVectorKeys = Map.of(
                data2, 0
        );
        assertThat(actualVectorKeys).isEqualTo(expectedVectorKeys);
    }

    @Override
    protected long totalBytesUsedByEntries(int count) {
        return emptyIndexBytesUsed() + count * (getVectorIndex().vectorsSupplier.heapBytesUsedPerEntry
                + VectorIndexMultipleKeys.HEAP_BYTES_USED_PER_NODE
                + VectorIndexMultipleKeys.HEAP_BYTES_PER_ONE_NODE_KEY_MAPPING)
                + getVectorIndex().indexBuilder.getGraph().ramBytesUsed()
                // if count is > 0, then subtract the fixed empty graph index cost we added in emptyIndexBytesUsed
                // since the graph index builder cost is already taken into account
                + (count == 0 ? 0 : -emptyGraphBytesUsed());
    }

    @Override
    protected long emptyIndexBytesUsed() {
        return AbstractVectorIndex.FIXED_HEAP_BYTES_USED + VectorIndexMultipleKeys.FIXED_HEAP_BYTES_USED
                + UpdatableVectorsSource.FIXED_HEAP_BYTES_USED
                // heap bytes used by empty graph index builder
                + emptyGraphBytesUsed();
    }
}
