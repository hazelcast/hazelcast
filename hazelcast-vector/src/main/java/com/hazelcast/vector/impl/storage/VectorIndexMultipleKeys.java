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

package com.hazelcast.vector.impl.storage;

import com.hazelcast.config.vector.Metric;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.vector.SearchResult;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.impl.DataSearchResult;
import com.hazelcast.vector.impl.SearchResultsImpl;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.jctools.maps.NonBlockingHashMapLong;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.hazelcast.internal.util.JVMUtil.OBJECT_HEADER_SIZE;
import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;

/**
 * Implementation of a vector index where each vector corresponds to multiple keys.
 * If the same vector is added, the deduplication process will occur.
 * As a result, no new vector will be added; instead, a reference from the old vector to the new key will be established.
 */
@SuppressWarnings("checkstyle:magicnumber")
public class VectorIndexMultipleKeys extends AbstractVectorIndex {

    static final long FIXED_HEAP_BYTES_USED = 3L * REFERENCE_COST_IN_BYTES;

    // 1 reference to key Data + 1 Integer (object header + integer bytes) (in the keyToNodeIdMap)
    static final long HEAP_BYTES_USED_FOR_KEY_TO_NODEID_MAP_ENTRY = (long) REFERENCE_COST_IN_BYTES
            + OBJECT_HEADER_SIZE + Integer.BYTES;

    // 1 reference to float[] + 1 Integer (object header + integer bytes) (in the vectorToNodeIdMap)
    static final long HEAP_BYTES_USED_FOR_VECTOR_TO_NODEID_MAP_ENTRY = (long) REFERENCE_COST_IN_BYTES
            + OBJECT_HEADER_SIZE + Integer.BYTES;

    // bytes used by each new node that is added to the index (key->nodeId + vector->nodeId mapping)
    static final long HEAP_BYTES_USED_PER_NODE = HEAP_BYTES_USED_FOR_VECTOR_TO_NODEID_MAP_ENTRY
            + HEAP_BYTES_USED_FOR_KEY_TO_NODEID_MAP_ENTRY;

    // fixed cost of nodeId -> VectorKeysEntry mapping when one key is mapped to the nodeId:
    // 1 long + 1 reference to VectorKeysEntry (in the nodeIdToKeyMap)
    // 1 Object Header of VectorKeysEntry
    // 1 reference to one Data key in the VectorKeysEntry
    static final long HEAP_BYTES_PER_ONE_NODE_KEY_MAPPING = Long.BYTES + OBJECT_HEADER_SIZE + 2L * REFERENCE_COST_IN_BYTES;

    // heap bytes used by each additional key mapped to a nodeId, when multiple keys are mapped to a single nodeId
    // we only take into account the cost of the reference, disregarding overhead of the
    // collection data structure used in VectorMultiKeysEntry
    static final long HEAP_BYTES_PER_ADDITIONAL_NODE_KEY_MAPPING = REFERENCE_COST_IN_BYTES;

    private final NonBlockingHashMapLong<VectorKeysEntry> nodeIdToKeyMap;
    private Map<Data, Integer> keyToNodeIdMap;
    private final Map<float[], Integer> vectorToNodeIdMap;

    public VectorIndexMultipleKeys(
            String indexName,
            Metric metric,
            int maxDegree,
            int efConstruction,
            int dimensions
    ) {
        super(indexName, metric, maxDegree, efConstruction, dimensions);
        nodeIdToKeyMap = new NonBlockingHashMapLong<>(1024);
        // `keyToNodeIdMap` is used only on partition thread
        keyToNodeIdMap = new HashMap<>(1024);
        // TODO: vectorToNodeIdMap does not have to be concurrent, it is used only on partition thread
        vectorToNodeIdMap = new ConcurrentSkipListMap<>(Arrays::compare);
    }

    @Override
    protected VectorFloat<?> putInternal(Data key, VectorFloat<?> vector) {
        // todo: change for hd
        float[] vectorArray = (float[]) vector.get();
        var currentNodeId = vectorToNodeIdMap.get(vectorArray);
        if (currentNodeId == null) {
            // Order of the operations is important to avoid duplicates in search results.
            // See comments in VectorIndexSingleKey.putInternal.
            var updateResult = updateKeyMapping(key, idGenerator++);
            int nodeId = updateResult.nodeId();

            // nodeId mapping must be ready before vector is visible for searches
            // New mapping must be added after `deleteReferenceToKeyFromNodeId`, so `maybeRecreateIndex` works correctly.
            vectorToNodeIdMap.put(vectorArray, nodeId);
            vectorsSupplier.add(nodeId, vector);

            // make new node visible for search
            indexBuilder.addGraphNode(nodeId, vector);

            return updateResult.previousVector();
        }
        var updateResult = updateKeyMapping(key, currentNodeId);
        assert updateResult.nodeId() == currentNodeId : "Should not change node id if the node already exists";
        return updateResult.previousVector();
    }

    @Override
    protected SearchResults<Data, Data> toDataSearchResults(
            io.github.jbellis.jvector.graph.SearchResult searchResult,
            int limit,
            ConcurrentModificationPolicy cmPolicy
    ) {
        List<SearchResult<Data, Data>> resultsList = new ArrayList<>();
        for (var nodeScore : searchResult.getNodes()) {
            if (resultsList.size() >= limit) {
                break;
            }
            var keyData = nodeIdToKeyMap.get(nodeScore.node);
            if (keyData != null) {
                var addedCount = keyData.forEachKeyWithLimit(
                        limit - resultsList.size(),
                        key -> resultsList.add(new DataSearchResult(nodeScore.node, key, nodeScore.score))
                );
                if (addedCount == 0) {
                    // We got VectorKeysEntry that was emptied concurrently with search or postprocessing.
                    // This might lead to returning fewer results than expected.
                    // If it was not emptied but just concurrently decreased in size it is not a problem,
                    // as we search underlying JVector index with limit assuming 1 key per node id.
                    cmPolicy.action();
                }
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

    /**
     * Logically deletes node from index.
     *
     * @param nodeId Existing node
     */
    private void deleteNode(int nodeId) {
        // todo for hd: this should be change during vector hd implementation
        var vectorArray = (float[]) vectorsSupplier.getVector(nodeId).get();
        vectorToNodeIdMap.remove(vectorArray);
        indexBuilder.markNodeDeleted(nodeId);

        // remove mapping after the node was deleted from vector index
        // - now it should no longer be returned in searches.
        nodeIdToKeyMap.remove(nodeId);
    }

    @Override
    protected boolean deleteInternal(Data key) {
        Integer nodeId = keyToNodeIdMap.remove(key);
        if (nodeId == null) {
            return false;
        }

        deleteReferenceToKeyFromNodeId(key, nodeId);
        maybeRecreateIndex();
        return true;
    }

    /**
     * Removes the previous reference to the key from nodeIdToKeyMap.
     */
    private void deleteReferenceToKeyFromNodeId(Data key, int nodeId) {
        var indexToKeys = nodeIdToKeyMap.get(nodeId);

        if (indexToKeys == null) {
            return;
        }

        indexToKeys.deleteKey(key);
        if (indexToKeys.isEmptyKeys()) {
            // The vector is no longer associated with any key.
            // The vector will be deleted from the supplier during cleanup.
            deleteNode(nodeId);
        }
    }

    private record UpdateKeyMappingResult(int nodeId, VectorFloat<?> previousVector) {}

    /**
     * Update key mapping in the nodeIdToKeyMap and keyToNodeIdMap.
     * @param newNodeId id of the new node. May be changed if the index is recreated during this operation
     * @return actual node id used and the previous vector corresponding to the key, if any.
     */
    private UpdateKeyMappingResult updateKeyMapping(Data key, int newNodeId) {
        var previousNodeId = keyToNodeIdMap.put(key, newNodeId);
        // This check prevents duplicate key from being added to nodeIdToKeyMap.keys.
        if (previousNodeId != null && previousNodeId == newNodeId) {
            return new UpdateKeyMappingResult(newNodeId, vectorsSupplier.getVector(previousNodeId));
        }

        VectorFloat<?> previousVector = null;

        // first delete old mapping, then add new to avoid duplicates in search results
        if (previousNodeId != null) {
            previousVector = vectorsSupplier.getVector(previousNodeId);
            deleteReferenceToKeyFromNodeId(key, previousNodeId);
            if (maybeRecreateIndex()) {
                // uncommon case of recreating the index
                // fix just added keyToNodeIdMap entry with smaller node id
                int smallerNewNodeId = idGenerator++;
                Integer prevNodeId = keyToNodeIdMap.put(key, smallerNewNodeId);
                assert prevNodeId != null && prevNodeId == newNodeId : "Mapping changed concurrently during put operation";
                newNodeId = smallerNewNodeId;
            }
        }

        addKeyMapping(key, newNodeId);
        return new UpdateKeyMappingResult(newNodeId, previousVector);
    }

    private void addKeyMapping(Data key, int newNodeId) {
        computeNonThreadSafe(nodeIdToKeyMap, newNodeId, (k, v) -> (v == null) ? new VectorOneKeyEntry(key) : v.addKey(key));
    }

    private interface LongObjBiFunction<V> {
        V apply(long key, V currentValue);
    }

    /**
     * Non-thread safe {@link Map#compute} for {@link NonBlockingHashMapLong}.
     * NonBlockingHashMapLong lacks newer Map methods specializations without boxing.
     * @implNote guarantees that remappingFunction is invoked exactly once
     */
    private static <V> V computeNonThreadSafe(NonBlockingHashMapLong<V> map, long key,
                                              LongObjBiFunction<V> remappingFunction) {
        // Similar implementation as default in Map.
        Objects.requireNonNull(remappingFunction);
        V oldValue = map.get(key);

        V newValue = remappingFunction.apply(key, oldValue);
        if (newValue == null) {
            if (oldValue != null) {
                // delete mapping if it existed
                map.remove(key);
            }
        } else if (newValue != oldValue) {
            // add new mapping or replace old mapping
            map.put(key, newValue);
        }
        return newValue;
    }

    @Override
    public long heapBytesUsed() {
        return FIXED_HEAP_BYTES_USED + super.heapBytesUsed()
                // bytes used by vector -> nodeId mappings
                + vectorToNodeIdMap.size() * HEAP_BYTES_USED_FOR_VECTOR_TO_NODEID_MAP_ENTRY
                // bytes used by key -> nodeId mappings
                + keyToNodeIdMap.size() * HEAP_BYTES_USED_FOR_KEY_TO_NODEID_MAP_ENTRY
                // bytes used by nodeId -> key/keys mappings
                // since some nodeId's may be mapped to multiple Data keys (in VectorMultiKeysEntry)
                + nodeIdToKeyMap.size() * HEAP_BYTES_PER_ONE_NODE_KEY_MAPPING
                + (keyToNodeIdMap.size() - nodeIdToKeyMap.size()) * HEAP_BYTES_PER_ADDITIONAL_NODE_KEY_MAPPING;
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

        // rebuild nodeIdToKeyMap
        assert nodeIdToKeyMap.isEmpty() : "Migration to non empty index";
        keyToNodeIdMap.forEach(this::addKeyMapping);

        // rebuild vectorToNodeIdMap
        assert vectorToNodeIdMap.isEmpty() : "Migration to non empty index";
        vectorsSupplier.entries().forEach(
                entry -> vectorToNodeIdMap.put((float[]) entry.getValue().get(), entry.getKey().intValue())
        );
    }

    //endregion
}
