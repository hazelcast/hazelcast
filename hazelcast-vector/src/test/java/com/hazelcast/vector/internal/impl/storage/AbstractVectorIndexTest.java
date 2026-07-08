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
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.vector.IndexMutationDisallowedException;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.impl.InternalSearchResult;
import com.hazelcast.vector.internal.impl.VectorTestUtils;
import io.github.jbellis.jvector.util.RamUsageEstimator;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.vector.internal.impl.VectorTestUtils.randomData;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.randomVectorFloat;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class AbstractVectorIndexTest {
    protected Data data1 = new HeapData(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
    protected Data data2 = new HeapData(new byte[] {2, 3, 4, 5, 6, 7, 8, 1});
    protected Data data3 = new HeapData(new byte[] {3, 4, 5, 6, 7, 8, 1, 2});
    protected UUID uuid = UUID.randomUUID();

    abstract AbstractVectorIndex getVectorIndex();

    @Test
    void testPutAndReplace() {
        // put vector with id 1
        getVectorIndex().put(data2, createVector(3, 1));
        // put vector with id 2
        getVectorIndex().put(data1, createVector(1, 1));
        // replace vector with id 2
        getVectorIndex().put(data1, createVector(2, 1));

        assertThat(getVectorIndex().get(data1)).isEqualTo(createVector(2, 1));

        assertTrue(getVectorIndex().indexBuilder.getGraph().containsNode(0));
        assertTrue(getVectorIndex().indexBuilder.getGraph().containsNode(1));
        assertTrue(getVectorIndex().indexBuilder.getGraph().containsNode(2));

        cleanupIndex();

        assertTrue(getVectorIndex().indexBuilder.getGraph().containsNode(0));
        assertFalse(getVectorIndex().indexBuilder.getGraph().containsNode(1));
        assertTrue(getVectorIndex().indexBuilder.getGraph().containsNode(2));
    }

    @Test
    void testPutShouldReturnPreviousVector() {
        assertThat(getVectorIndex().put(data1, createVector(1, 1), true)).isNull();
        assertThat(getVectorIndex().put(data1, createVector(2, 1), true)).isEqualTo(createVector(1, 1));
        assertThat(getVectorIndex().get(data1)).isEqualTo(createVector(2, 1));

        cleanupIndex();

        assertThat(getVectorIndex().get(data1)).isEqualTo(createVector(2, 1));
    }

    @Test
    void testDelete() {
        getVectorIndex().put(data1, createVector(-1, 0));
        getVectorIndex().put(data2, createVector(1, 0));
        getVectorIndex().put(data3, createVector(1, 1));
        getVectorIndex().delete(data2);

        assertThatThrownBy(() -> getVectorIndex().get(data2))
                .as("Should throw when the entry does not exist")
                .isInstanceOf(IllegalStateException.class);

        assertTrue(getVectorIndex().indexBuilder.getGraph().containsNode(0));
        assertTrue(getVectorIndex().indexBuilder.getGraph().containsNode(1));
        assertTrue(getVectorIndex().indexBuilder.getGraph().containsNode(2));

        var results = getVectorIndex().search(createVector(0, 2), 10);
        var actual = toKeyNodeIdMap(results);
        var expected = Map.of(
                data1, 0,
                data3, 2
        );
        assertThat(actual).isEqualTo(expected);

        cleanupIndex();

        assertTrue(getVectorIndex().indexBuilder.getGraph().containsNode(0));
        assertFalse(getVectorIndex().indexBuilder.getGraph().containsNode(1));
        assertTrue(getVectorIndex().indexBuilder.getGraph().containsNode(2));
    }

    @Test
    void testDeleteAddTheSameVector() {
        getVectorIndex().put(data1, createVector(1, 0));
        getVectorIndex().put(data2, createVector(2, 0));
        getVectorIndex().delete(data2);
        getVectorIndex().put(data2, createVector(2, 0));

        var results = getVectorIndex().search(createVector(0, 2), 10);
        var actual = toKeyNodeIdMap(results);
        var expected = Map.of(
                data1, 0,
                data2, 2
        );
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testDeleteWithCleanupAndAddTheSameVector() {
        getVectorIndex().put(data1, createVector(1, 0));
        getVectorIndex().put(data2, createVector(2, 0));
        getVectorIndex().delete(data2);
        cleanupIndex();
        getVectorIndex().put(data2, createVector(2, 0));

        var results = getVectorIndex().search(createVector(0, 2), 10);
        var actual = toKeyNodeIdMap(results);
        var expected = Map.of(
                data1, 0,
                data2, 2
        );
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testSearch() {
        getVectorIndex().put(data1, createVector(-1, 0));
        getVectorIndex().put(data2, createVector(1, 0));
        getVectorIndex().put(data3, createVector(1, 1));
        var results = getVectorIndex().search(createVector(0.8f, 0.8f), 3);
        assertEquals(3, results.size());
        var resultsMap = toMap(results);
        assertThat(resultsMap.keySet()).containsExactly(data3, data2, data1);
    }

    @Test
    void testSearchWithLimit() {
        getVectorIndex().put(data1, createVector(-1, 0));
        getVectorIndex().put(data2, createVector(1, 0));
        getVectorIndex().put(data3, createVector(1, 1));
        var results = getVectorIndex().search(createVector(0.8f, 0.8f), 2);
        assertEquals(2, results.size());
        var resultsMap = toMap(results);
        assertThat(resultsMap.keySet()).containsExactly(data3, data2);
    }

    @Test
    void testSearchInvalidLimit() {
        assertThatThrownBy(() -> getVectorIndex().search(createVector(0.8f, 0.8f), 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSearchEmptySet() {
        var results = getVectorIndex().search(createVector(0.8f, 0.8f), 1);
        assertEquals(0, results.size());
    }

    @Test
    void testSearchAfterReplace() {
        getVectorIndex().put(data1, createVector(-1, 0));
        getVectorIndex().put(data2, createVector(1, 0));

        // replace - new vector will be the closest neighbor
        getVectorIndex().put(data1, createVector(1, 1));

        for (int i = 0; i < 2; ++i) {
            var results = getVectorIndex().search(createVector(0.8f, 0.8f), 3);
            assertEquals(2, results.size());
            var resultsMap = toMap(results);
            assertThat(resultsMap.keySet()).containsExactly(data1, data2);

            // test before and after cleanup
            cleanupIndex();
        }
    }

    @Test
    void testSearchAfterDelete() {
        getVectorIndex().put(data1, createVector(-1, 0));
        getVectorIndex().put(data2, createVector(1, 0));
        getVectorIndex().put(data3, createVector(1, 1));
        getVectorIndex().delete(data2);

        for (int i = 0; i < 2; ++i) {
            var results = getVectorIndex().search(createVector(0.8f, 0.8f), 3);
            assertEquals(2, results.size());
            var resultsMap = toMap(results);
            assertThat(resultsMap.keySet()).containsExactly(data3, data1);

            // test before and after cleanup
            cleanupIndex();
        }
    }

    @Test
    void testLockedUnlockedIndex() {
        assertThatNoException().isThrownBy(() -> getVectorIndex().lockIndexMutation(uuid));
    }

    @Test
    void testLockedIndex() {
        getVectorIndex().lockIndexMutation(uuid);

        assertThatThrownBy(() -> getVectorIndex().checkMutatingOperationAllowed())
                .isInstanceOf(IndexMutationDisallowedException.class)
                .hasMessage("Index optimization process is in progress.");
        assertThatThrownBy(() -> getVectorIndex().lockIndexMutation(UUID.randomUUID()))
                .isInstanceOf(IndexMutationDisallowedException.class)
                .hasMessageStartingWith("Index optimization process").hasMessageEndingWith("is in progress.");

        assertThatNoException().isThrownBy(() -> getVectorIndex().cleanup());
        assertThatNoException().isThrownBy(() -> getVectorIndex().unlockIndexMutation());
        assertThatNoException().isThrownBy(() -> getVectorIndex().checkMutatingOperationAllowed());
    }

    @Test
    void testUnlockedIndex() {
        assertThatNoException().isThrownBy(() -> getVectorIndex().checkMutatingOperationAllowed());

        assertThatThrownBy(() -> getVectorIndex().unlockIndexMutation())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Failed to unlock a collection that was not locked.");

        assertThatThrownBy(() -> getVectorIndex().cleanup())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The index must be locked for mutation before the optimization process begins.");
    }

    @Test
    void testCleanupCanBeDoneOncePerLock() {
        getVectorIndex().lockIndexMutation(uuid);
        getVectorIndex().cleanup();
        assertThatThrownBy(() -> getVectorIndex().cleanup())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Optimization already done, lock is not reusable");
    }

    @Test
    void putTheDifferentVectorWithTheSameKey() {
        // prevent graph recreation during update
        getVectorIndex().put(data2, createVector(1, 1));

        getVectorIndex().put(data1, createVector(1, 0));
        getVectorIndex().put(data1, createVector(0, 1));

        assertGraphContainsExactNodesId(Set.of(0, 1, 2));

        var results = getVectorIndex().search(createVector(1, 1), 3);
        var actualData = toKeyNodeIdMap(results);
        Map<Object, Integer> expected = Map.of(
                data2, 0,
                data1, 2
        );
        assertThat(actualData).isEqualTo(expected);

        getVectorIndex().indexBuilder.cleanup();
        assertGraphContainsExactNodesId(Set.of(0, 2));
    }

    @Test
    void putVectorAfterRemovingAll() {
        getVectorIndex().put(data1, createVector(1, 0));
        assertThat(getVectorIndex().getSearchLogicalTime()).as("Initial index version should be 0").isZero();
        getVectorIndex().delete(data1);
        assertThat(getVectorIndex().getSearchLogicalTime()).as("Should increase index version").isOne();
        getVectorIndex().put(data2, createVector(0, 1));

        var results = getVectorIndex().search(createVector(1, 1), 3);
        Map<Data, Integer> actualData = toKeyNodeIdMap(results);
        // idGenerator should be reset when the collection becomes empty
        Map<Data, Integer> expected = Map.of(
                data2, 0
        );
        assertThat(actualData).isEqualTo(expected);
    }

    @Test
    void nodeIdIsNotReusedAfterOverwritingTheLastEntry() {
        var expectedVector = createVector(2, 0);
        getVectorIndex().put(data1, createVector(1, 0));
        assertThat(getVectorIndex().getSearchLogicalTime()).as("Initial index version should be 0").isZero();
        getVectorIndex().put(data1, expectedVector); // implicit delete of the key data1
        assertThat(getVectorIndex().getSearchLogicalTime()).as("Should increase index version").isOne();
        getVectorIndex().put(data2, createVector(3, 0));
        getVectorIndex().put(data3, createVector(4, 0));

        assertThat(getVectorIndex().get(data1)).isEqualTo(expectedVector);

        var results = getVectorIndex().search(createVector(1, 1), 10);
        assertThat(results.size()).isEqualTo(3);
    }

    @Test
    void testSearchWithLimitSameVector() {
        getVectorIndex().put(data1, createVector(1, 0));
        getVectorIndex().put(data2, createVector(1, 0));
        getVectorIndex().put(data3, createVector(1, 0));
        var results = getVectorIndex().search(createVector(0.8f, 0.8f), 2);
        var resultsMap = toMap(results);
        assertThat(resultsMap.keySet()).size().isEqualTo(2);
        assertThat(resultsMap.keySet()).isSubsetOf(data1, data2, data3);
    }

    @Test
    void indexLocked_then_mutableOperationsFails() {
        getVectorIndex().lockIndexMutation(uuid);

        assertThatThrownBy(() -> getVectorIndex().put(data1, createVector(1, 0)))
                .isInstanceOf(IndexMutationDisallowedException.class)
                .hasMessageContaining("Index optimization process is in progress.");

        assertThatThrownBy(() -> getVectorIndex().put(data1, createVector(1, 0), true))
                .isInstanceOf(IndexMutationDisallowedException.class)
                .hasMessageContaining("Index optimization process is in progress.");

        assertThatThrownBy(() -> getVectorIndex().delete(data1))
                .isInstanceOf(IndexMutationDisallowedException.class)
                .hasMessageContaining("Index optimization process is in progress.");
    }

    @Test
    void testHeapBytesUsed_whenEmpty() {
        assertEquals(emptyIndexBytesUsed(), getVectorIndex().heapBytesUsed());
    }

    @Test
    void testHeapBytesUsed_whenEntriesAdded() {
        for (int i = 0; i < 100; i++) {
            getVectorIndex().put(randomData(16), randomVectorFloat(2));
        }
        assertEquals(totalBytesUsedByEntries(100), getVectorIndex().heapBytesUsed());
    }

    @Test
    void testHeapBytesUsed_whenSameEntryAddedMultipleTimes() {
        Data key = randomData(16);
        var vector = randomVectorFloat(2);
        for (int i = 0; i < 100; i++) {
            getVectorIndex().put(key, vector);
        }
        getVectorIndex().lockIndexMutation(uuid);
        getVectorIndex().cleanup();
        assertEquals(totalBytesUsedByEntries(1), getVectorIndex().heapBytesUsed());
    }

    @Test
    void testHeapBytesUsed_whenEntriesAddedSomeRemoved() {
        Data[] keys = new Data[100];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = randomData(16);
            getVectorIndex().put(keys[i], randomVectorFloat(2));
        }
        getVectorIndex().delete(keys[0]);
        getVectorIndex().lockIndexMutation(uuid);
        getVectorIndex().cleanup();
        assertEquals(totalBytesUsedByEntries(99), getVectorIndex().heapBytesUsed());
    }

    // return heap bytes used by count entries
    protected abstract long totalBytesUsedByEntries(int count);

    // return heap bytes used by empty index
    protected abstract long emptyIndexBytesUsed();

    protected long emptyGraphBytesUsed() {
        int OH_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
        int REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        int AH_BYTES = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
        return (long) OH_BYTES + (long) REF_BYTES * 2L + (long) AH_BYTES;
    }

    protected void assertGraphContainsExactNodesId(Set<Integer> expectedNodes) {
        Set<Integer> actualNodes = new HashSet<>();
        var nodesIt = getVectorIndex().indexBuilder.getGraph().getNodes();
        nodesIt.forEachRemaining((int node) -> actualNodes.add(node));
        assertThat(actualNodes).isEqualTo(expectedNodes);
    }

    protected Map<Data, Integer> toKeyNodeIdMap(SearchResults<Data, Data> results) {
        return toMap(results, sr -> ((InternalSearchResult<Data, Data>) sr).id());
    }

    private void cleanupIndex() {
        getVectorIndex().lockIndexMutation(uuid);
        getVectorIndex().cleanup();
        getVectorIndex().unlockIndexMutation();
    }

    protected VectorFloat<?> createVector(float... coordinates) {
        return VectorTestUtils.toVectorFloat(coordinates);
    }
}
