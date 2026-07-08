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

import com.hazelcast.config.vector.Metric;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.JVMUtil;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.vector.SearchResult;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.impl.DataSearchResult;
import com.hazelcast.vector.internal.impl.SearchResultsImpl;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.jctools.maps.NonBlockingHashMapLong;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

/**
 * Vector index implementation where each vector corresponds precisely to one key.
 * If a vector with the same coordinates is added,
 * it will result in a new duplicated vector with a newly generated node ID.
 * <p>
 * Index as a whole consists of the following elements:
 * <ul>
 *     <li>Bidirectional mapping between entry key and nodeId</li>
 *     <li>Vector storage ({@link #vectorsSupplier}: nodeId -> float[]</li>
 *     <li>JVector graph index builder which gets all updates</li>
 *     <li>JVector graph searcher</li>
 * </ul>
 * {@link VectorIndexSingleKey} orchestrates collaboration between the components.
 * {@link VectorIndexSingleKey} tries to keep the components in consistent state, but this may be not possible in case of
 * unexpected exceptions.
 */
@SuppressWarnings("checkstyle:magicnumber")
public class VectorIndexSingleKey extends AbstractVectorIndex {

    static final long FIXED_HEAP_BYTES_USED = 2L * JVMUtil.REFERENCE_COST_IN_BYTES;

    // For each node that is added to the index the cost is:
    // 1 long + 1 reference to key Data (in the nodeIdToKeyMap)
    // 1 reference to key Data + 1 Integer (object header + integer bytes) (in the keyToNodeIdMap)
    // Size of Data keys are only referenced by this class' collections. The actual size of Data keys
    // is taken into account in VectorCollectionStorage which includes the Storage instance where keys
    // and values are stored.
    // We do not take into account actual memory usage by the NonBlockingHashMap,
    // so the estimate returned by heapBytesUsed() will be lower than actual heap usage.
    static final long HEAP_BYTES_USED_PER_ENTRY = Long.BYTES + 2L * JVMUtil.REFERENCE_COST_IN_BYTES
            + JVMUtil.OBJECT_HEADER_SIZE + Integer.BYTES;

    private final NonBlockingHashMapLong<Data> nodeIdToKeyMap;
    private Map<Data, Integer> keyToNodeIdMap;

    public VectorIndexSingleKey(
            String indexName,
            Metric metric,
            int maxDegree,
            int efConstruction,
            int dimensions,
            ForkJoinPool parallelExecutor
    ) {
        super(indexName, metric, maxDegree, efConstruction, dimensions, parallelExecutor);
        nodeIdToKeyMap = new NonBlockingHashMapLong<>(1024);
        // `keyToNodeIdMap` is used only on partition thread
        keyToNodeIdMap = new HashMap<>(1024);
    }

    @Override
    protected VectorFloat<?> putInternal(Data key, VectorFloat<?> vector) {
        int nodeId = idGenerator++;

        // Order of the operations is important. We do not want to return duplicated keys in searches
        // but the consequence is that concurrently updated entry may be missing in search results.
        // This should be a reasonable compromise as search is approximate anyway and in line
        // with lack of consistency guarantees for search results.
        //
        // First remove old mapping then add new mapping, so 2 nodeIds never map to the same key.
        // This guarantees that no duplicates will be returned. Transiently `keyToNodeIdMap` points
        // to not yet existing `nodeId`, but no concurrent operation depends on that.
        Integer previousNodeId = keyToNodeIdMap.put(key, nodeId);
        VectorFloat<?> previousVector = null;
        if (previousNodeId != null) {
            previousVector = vectorsSupplier.getVector(previousNodeId);
            deleteNode(previousNodeId);
            if (maybeRecreateIndex()) {
                // uncommon case of recreating the index
                // fix just added keyToNodeIdMap entry with smaller node id
                int newNodeId = idGenerator++;
                int prevNodeId = keyToNodeIdMap.put(key, newNodeId);
                assert prevNodeId == nodeId : "Mapping changed concurrently during put operation";
                nodeId = newNodeId;
            }
        }
        // nodeId mapping must be ready before vector is visible for searches
        // New mapping must be added after `deleteNode`, so `maybeRecreateIndex` works correctly.
        nodeIdToKeyMap.put(nodeId, key);
        vectorsSupplier.add(nodeId, vector);

        // make new node visible for search
        indexBuilder.addGraphNode(nodeId, vector);

        return previousVector;
    }

    @Override
    protected SearchResults<Data, Data> toDataSearchResults(
            io.github.jbellis.jvector.graph.SearchResult searchResult,
            int limit,
            ConcurrentModificationPolicy cmPolicy
    ) {
        List<SearchResult<Data, Data>> resultsList = new ArrayList<>();
        for (var nodeScore : searchResult.getNodes()) {
            var keyData = nodeIdToKeyMap.get(nodeScore.node);
            if (keyData != null) {
                resultsList.add(new DataSearchResult(nodeScore.node, keyData, nodeScore.score));
            } else {
                cmPolicy.action();
            }
        }
        return new SearchResultsImpl<>(resultsList);
    }

    @Override
    protected Integer getNodeIdByKey(Data key) {
        return keyToNodeIdMap.get(key);
    }

    @Override
    protected boolean hasLiveNodes() {
        return !nodeIdToKeyMap.isEmpty();
    }

    @Override
    protected boolean deleteInternal(Data key) {
        Integer node = keyToNodeIdMap.remove(key);
        if (node == null) {
            return false;
        }

        deleteNode(node);
        maybeRecreateIndex();
        return true;
    }

    /**
     * Logically deletes node from index.
     *
     * @param nodeId Existing node
     */
    private void deleteNode(int nodeId) {
        indexBuilder.markNodeDeleted(nodeId);

        // remove mapping after the node was deleted from vector index
        // - now it should no longer be returned in searches,
        // but in unlikely case it could have been just return and still be post-processed.
        // In such case concurrent modification will be detected.
        //
        // Do not remove it from vectorsSupplier yet as it may be needed as previous value for put
        // and still may be referenced in JVector graph.
        nodeIdToKeyMap.remove(nodeId);
    }

    @Override
    public long heapBytesUsed() {
        long heapBytesUsed = FIXED_HEAP_BYTES_USED + super.heapBytesUsed();
        return heapBytesUsed + nodeIdToKeyMap.size() * HEAP_BYTES_USED_PER_ENTRY;
    }

    //region migration support

    @Override
    void writeKeyToNodeIdMapping(ObjectDataOutput out, Data key) throws IOException {
        out.writeInt(keyToNodeIdMap.get(key));
    }

    @Override
    void resetState(ReplicationStateHolder.CollectionReplicationStateHolder.IndexReplicationStateHolder indexState) {
        super.resetState(indexState);

        assert keyToNodeIdMap.isEmpty() : "Migration to non empty index";
        keyToNodeIdMap = indexState.keyToNodeIdMap;

        assert nodeIdToKeyMap.isEmpty() : "Migration to non empty index";
        keyToNodeIdMap.forEach((key, nodeId) -> nodeIdToKeyMap.put(nodeId, key));
    }

    //endregion
}
