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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.internal.memory.Measurable;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.map.impl.recordstore.StorageImpl;
import com.hazelcast.map.impl.recordstore.expiry.ExpirySystem;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.VectorCollectionMergeTypes;
import com.hazelcast.vector.IndexMutationDisallowedException;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchResult;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.impl.DataVectorDocument;
import com.hazelcast.vector.impl.InternalSearchResult;
import com.hazelcast.vector.internal.impl.VectorCollectionSerializerHook;
import com.hazelcast.vector.internal.impl.VectorUtil;
import com.hazelcast.vector.internal.impl.stats.OnDemandStats;
import com.hazelcast.vector.internal.impl.stats.OnDemandStatsImpl;
import com.hazelcast.vector.internal.impl.stats.VectorIndexStats;
import com.hazelcast.vector.internal.impl.stats.VectorIndexStatsImpl;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.JVMUtil.OBJECT_HEADER_SIZE;
import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;
import static com.hazelcast.vector.internal.impl.Hints.EF_SEARCH;
import static com.hazelcast.vector.internal.impl.Hints.PARTITION_LIMIT;

/**
 * Storage for {@link VectorDocument} metadata and vectors.
 * <p>
 * One instance of {@link VectorCollectionStorage} stores data per {@link VectorCollection} per partition.
 * <p>
 * This class is not thread-safe except for {@link #search} method which is allowed concurrently with other operations.
 *
 * <h2>Index optimization synchronization</h2>
 * Each mutating operation requires that no optimization process is currently being performed on any index.
 * If any index is undergoing an optimization process,
 * the mutating method will throw {@link IndexMutationDisallowedException}.
 * If an index is already undergoing an optimization process,
 * attempting to start a second optimization process on the same index
 * will result an exception {@link IndexMutationDisallowedException}.
 * Multiple optimization processes can be performed simultaneously on different indexes.
 *
 * <h2>Storage format</h2>
 * Currently, keys and values are stored in RecordStore as {@link Data} - equivalent to {@link InMemoryFormat#BINARY}.
 * However, in the future {@link InMemoryFormat#OBJECT} format may be implemented.
 * Because of that the following conventions are used:
 * <ol>
 * <li>{@link VectorCollectionStorage} expects {@code Data} as key/value and returns {@code VectorDocument<Data>}.
 *     {@link DataVectorDocument} is not declared as return type to ease the transition to OBJECT format.</li>
 * <li>Operations do not need to do any extra serialization of the returned {@code VectorDocument<Data>}
 *     - all implementations are serializable</li>
 * <li>Message tasks must convert {@code VectorDocument<Data>} to {@link DataVectorDocument} using
 *     {@link VectorUtil#serialize}. To avoid extra garbage {@link VectorCollectionStorage}
 *     returns {@link DataVectorDocument}, but that is an implementation detail and may change in the future.</li>
 * <li>Proxies are responsible for deserializing {@code VectorDocument<Data>} to type expected by the user.</li>
 * <li>{@code VectorDocumentImpl<Data>} and {@code DataVectorDocument} are almost equivalent, but have different
 *     serialization class id so should not be conflated in type-unsafe code. It is safest to use
 *     {@code VectorDocument<Data>} where possible</li>
 * </ol>
 *
 * @implNote This class tries to perform updates atomically: first validates then executes the operation.
 *           However, unexpected exceptions may leave {@link RecordStore} and {@link AbstractVectorIndex}
 *           not consistent with each other.
 */
@SuppressWarnings("checkstyle:methodCount")
public class VectorCollectionStorage implements Measurable {
    /**
     * Placeholder object returned from {@link #findLockedIndex()} when no index is locked.
     */
    public static final Object NOT_LOCKED = new Object();

    private static final long FIXED_HEAP_BYTES_USED = OBJECT_HEADER_SIZE + 7L * REFERENCE_COST_IN_BYTES
            // int partitionId, logicalTime
            + 2 * Integer.BYTES
            // boolean binaryMetadataFormat
            + 1;
    private static final int MAX_CONCURRENT_MODIFICATION_RETRIES = 3;
    protected final ILogger logger;
    private final NodeEngine nodeEngine;
    private final VectorCollectionConfig config;
    private final String name;
    // assume Storage lifecycle is tied to VectorCollectionStorage lifecycle
    private final Storage<Data, Record<Data>> recordStore;
    private final VectorCollectionObjectProvider.VectorFloatConverter vectorConverter;

    private final int partitionId;
    private final boolean binaryMetadataFormat;
    private final VectorIndexHolder vectorIndexes;

    /**
     * Logical time for optimistic read concurrency for searches.
     * Updated only on partition thread, but read also by searches.
     * <p>
     * Protocol:
     * <ul>
     * <li>every update of exising record increments logicalTime twice: before and after update
     * <li>odd logicalTime means that update in progress
     * <li>search obtains logical time at the beginning and if the result contains entry inserted/updated later,
     *     CME is triggered
     * <li>new Record is created with current logical time
     * <li>adding a Record does not increment logicalTime because it is published atomically and there are
     *     other mechanisms in place to handle new/deleted records
     * <li>updates of already existing entries increase logicalTime. It is practically free when entries are only
     *     inserted but never updated. The only extra cost in such case are version checks during searches.
     * <li>removing a record increases logicalTime to guarantee consistent results if a record with the same key
     *     is reinserted later. This impacts also records with different keys - if found, they will be treated as CME
     * <li>visibility and happens-before is provided by volatile variables and {@link Storage} operations
     * </ul>
     */
    private volatile int logicalTime;

    public VectorCollectionStorage(
            NodeEngine nodeEngine,
            String name,
            int partitionId,
            VectorCollectionConfig config,
            VectorCollectionObjectProvider memoryObjectProvider,
            VectorIndexFactory vectorIndexFactory
    ) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(VectorCollectionStorage.class);
        this.config = config;
        this.name = name;
        this.partitionId = partitionId;
        this.recordStore = new StorageImpl<>(InMemoryFormat.BINARY, ExpirySystem.NULL, nodeEngine.getSerializationService());
        this.binaryMetadataFormat = true;
        this.vectorIndexes = new VectorIndexHolder(config.getVectorIndexConfigs(), vectorIndexFactory);
        this.vectorConverter = memoryObjectProvider.createVectorFloatConverter();
    }

    public String getName() {
        return name;
    }

    public VectorCollectionConfig getConfig() {
        return config;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public VectorDocument<Data> put(Data keyData, Data userValue, VectorValues vectorValues) {
        checkMutatingOperationAllowed();
        validateVectors(vectorValues);

        VectorDocument<Data> oldDocument = null;
        var oldRecord = recordStore.get(keyData);
        Data oldValue = null;
        if (oldRecord == null) {
            recordStore.put(keyData, createRecord(userValue));
        } else {
            oldValue = oldRecord.getValue();
            // mark intention for update of existing record
            updateRecordVersion(oldRecord);
            recordStore.updateRecordValue(keyData, oldRecord, userValue);
        }
        var oldVectors = putVectorsReturningPrevious(keyData, vectorValues);

        assert (oldRecord == null && oldVectors == null) || (oldRecord != null && oldVectors != null)
                : "Record store inconsistent with vector index";

        if (oldRecord != null) {
            // update finished
            updateRecordVersion(oldRecord);
            oldDocument = new DataVectorDocument(oldValue, oldVectors);
        }
        return oldDocument;
    }

    public VectorDocument<Data> putIfAbsent(Data keyData, Data userValue, VectorValues vectorValues) {
        checkMutatingOperationAllowed();
        validateVectors(vectorValues);

        VectorDocument<Data> oldValue = get(keyData);
        if (oldValue != null) {
            return oldValue;
        }
        recordStore.put(keyData, createRecord(userValue));
        putVectors(keyData, vectorValues);
        return null;
    }

    public void set(Data keyData, Data userValue, VectorValues vectorValues) {
        checkMutatingOperationAllowed();
        validateVectors(vectorValues);

        var currentRecord = recordStore.get(keyData);
        if (currentRecord == null) {
            recordStore.put(keyData, createRecord(userValue));
            putVectors(keyData, vectorValues);
        } else {
            // mark intention for update of existing record
            updateRecordVersion(currentRecord);
            recordStore.updateRecordValue(keyData, currentRecord, userValue);
            putVectors(keyData, vectorValues);
            // update finished
            updateRecordVersion(currentRecord);
        }
    }

    private Record<Data> createRecord(Data userValue) {
        // TODO: use RecordFactory
        return new SimpleVectorRecord<>(userValue, logicalTime);
    }

    private void updateRecordVersion(Record<?> record) {
        int currentTime = logicalTime;
        // volatile write in record
        record.setVersion(currentTime + 1);

        // new logical time can be used by searches only after the record was updated
        // this important for the second update, which makes the record visible again
        logicalTime = currentTime + 1;
    }

    private void advanceLogicalTime() {
        int currentTime = logicalTime;
        logicalTime = currentTime + 2;
    }

    private int getSearchLogicalTime() {
        // this must be single volatile read
        int currentTime = logicalTime;
        // make the time even, so only records fully updated before search start are visible
        return currentTime & ~0x1;
    }

    private void validateVectors(VectorValues vectorValues) {
        if (vectorValues instanceof VectorValues.SingleVectorValues svv) {
            if (vectorIndexes.isMultiIndex()) {
                throw new IllegalArgumentException("Collection has " + vectorIndexes.getSize() + " indexes, cannot put "
                        + "a single vector");
            }
            vectorIndexes.getSingleIndex().validateVector(svv.vector());
        } else if (vectorValues instanceof VectorValues.MultiIndexVectorValues mvv) {
            if (vectorIndexes.getSize() != mvv.indexNameToVector().size()) {
                throw new IllegalArgumentException("Collection has " + vectorIndexes.getSize() + " indexes, cannot put "
                        + mvv.indexNameToVector().size() + " vectors");
            }
            var notExistsIndexes = mvv.indexNameToVector().keySet().stream()
                    .filter(index -> !vectorIndexes.doesIndexExist(index))
                    .toList();
            if (!notExistsIndexes.isEmpty()) {
                throw new IllegalArgumentException("Invalid vector names specified,"
                        + " the collection does not contain the requested indexes: " + notExistsIndexes);
            }
            mvv.indexNameToVector().forEach((name, vector) -> vectorIndexes.getIndex(name).validateVector(vector));
        }
    }

    private void putVectors(Data keyData, VectorValues vectorValues) {
        if (vectorValues instanceof VectorValues.SingleVectorValues svv) {
            vectorIndexes.getSingleIndex().put(keyData, vectorConverter.toVectorFloat(svv.vector()));
        } else if (vectorValues instanceof VectorValues.MultiIndexVectorValues mvv) {
            mvv.indexNameToVector().forEach(
                    (key, value) -> vectorIndexes.getIndex(key).put(keyData, vectorConverter.toVectorFloat(value))
            );
        } else {
            throw new UnsupportedOperationException("Unsupported VectorValues type");
        }
    }

    @Nullable
    private VectorValues putVectorsReturningPrevious(Data keyData, VectorValues vectorValues) {
        if (vectorValues instanceof VectorValues.SingleVectorValues svv) {
            VectorFloat<?> previous = vectorIndexes.getSingleIndex().put(
                    keyData,
                    vectorConverter.toVectorFloat(svv.vector()),
                    true
            );
            return previous != null ? onlyVector(previous) : null;
        } else if (vectorValues instanceof VectorValues.MultiIndexVectorValues mvv) {
            Map<String, float[]> previousVectors = mvv.indexNameToVector().entrySet().stream()
                    .map(entry -> {
                        VectorFloat<?> previous = vectorIndexes.getIndex(entry.getKey()).put(
                                keyData,
                                vectorConverter.toVectorFloat(entry.getValue()),
                                true
                        );
                        return previous != null
                                ? Map.entry(entry.getKey(), vectorConverter.toFloatArray(previous))
                                : null;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            assert previousVectors.isEmpty() || previousVectors.size() == vectorIndexes.getSize()
                    : "Inconsistent vector indexes, key was present only in " + previousVectors.size();
            return !previousVectors.isEmpty() ? VectorValues.of(previousVectors) : null;
        } else {
            throw new UnsupportedOperationException("Unsupported VectorValues type");
        }
    }

    public VectorDocument<Data> get(Data key) {
        var value = getInternal(key);
        if (value == null) {
            return null;
        }
        VectorValues vectorValues = getVectorValues(key);
        return new DataVectorDocument(value, vectorValues);
    }

    private VectorValues getVectorValues(Data key) {
        VectorValues vectorValues;
        if (!vectorIndexes.isMultiIndex()) {
            vectorValues = onlyVector(vectorIndexes.getSingleIndex().get(key));
        } else {
            Map<String, float[]> vectors = new HashMap<>(vectorIndexes.getSize());
            vectorIndexes.forEachIndex(index -> vectors.put(
                    index.indexName,
                    vectorConverter.toFloatArray(index.get(key))
            ));
            vectorValues = VectorValues.of(vectors);
        }
        return vectorValues;
    }

    /**
     * @return if the value existed
     */
    public boolean delete(Data key) {
        checkMutatingOperationAllowed();

        var oldRecord = recordStore.get(key);
        if (oldRecord != null) {
            recordStore.removeRecord(key, oldRecord);
            // entry existed, delete it from indexes
            vectorIndexes.forEachIndex(vectorIndex -> {
                var deleted = vectorIndex.delete(key);
                assert deleted : "Inconsistent vector indexes, key was not present in " + vectorIndex.indexName;
            });

            // logical time is increased _after_ operation. Record during deletion can be visible
            // and it is handled correctly. We have to guarantee that any record inserted afterward,
            // possibly with the same key, will be treated as concurrent modification if found by search.
            // This is quite heavy-handed, but otherwise we would have to use tombstones and clear them.
            // It assumes that delete/remove operations are rare.
            advanceLogicalTime();
        }

        return oldRecord != null;
    }

    public VectorDocument<Data> remove(Data key) {
        checkMutatingOperationAllowed();
        // this is inefficient implementation, but remove should be used rarely
        var oldValue = get(key);
        if (oldValue != null) {
            delete(key);
        }
        return oldValue;
    }

    public void optimize(String indexName) {
        vectorIndexes.validateAndGetIndex(indexName).cleanup();
    }

    private Data getInternal(Data key) {
        var record = recordStore.get(key);
        return record != null ? record.getValue() : null;
    }

    /**
     * Gets value associated with given key only if it was not updated after given readLogicalTime
     *
     * @param key entry key
     * @param readLogicalTime time at which value is requested
     * @return value or null if the entry does not exist or was updated after readLogicalTime
     *
     * @implNote this method can be invoked from any thread, concurrently with mutations
     */
    private Data getInternal(Data key, int readLogicalTime) {
        var record = recordStore.get(key);
        if (record == null) {
            return null;
        }

        // read value first, to ensure that it is not updated concurrently after version check.
        // note that Data is never updated in place
        Data value = record.getValue();

        // volatile read from record
        if (record.getVersion() > readLogicalTime) {
            return null;
        }

        return value;
    }

    /**
     * Returns vector from only vector index in this collection,
     * with name if index name is defined.
     */
    private VectorValues onlyVector(VectorFloat<?> value) {
        var name = vectorIndexes.getSingleIndex().indexName;
        float[] vector = vectorConverter.toFloatArray(value);
        return name != null ? VectorValues.of(name, vector) : VectorValues.of(vector);

    }

    /**
     * Convert a float[] array into an ArrayVectorFloat, independent of the type of memory used.
     * Used to convert input arguments into a VectorFloat. Assume that all input arguments load on-heap first.
     *
     * @param vector the float[] to be wrapped
     * @return the corresponding VectorFloat, which is equivalent to ArrayVectorFloat.
     */
    private VectorFloat<?> convertToOnHeapArrayVectorFloat(float[] vector) {
        return ArrayVectorProvider.getInstance().createFloatVector(vector);
    }

    public SearchResults<Data, Data> search(VectorValues vectors, SearchOptions searchOptions) {
        for (int attempt = 0; attempt < MAX_CONCURRENT_MODIFICATION_RETRIES - 1; attempt++) {
            try {
                return doSearch(vectors, searchOptions, ConcurrentModificationPolicy.THROW);
            } catch (ConcurrentModificationException cme) {
                // Note that we might catch also ConcurrentModificationException
                // from collections that are used if they are not thread safe,
                // but that should not happen.
                logger.fine("Search attempt " + attempt + " failed", cme);
            }
        }
        // last resort - skip concurrently modified entries
        return doSearch(vectors, searchOptions, ConcurrentModificationPolicy.SKIP);
    }

    private SearchResults<Data, Data> doSearch(
            VectorValues vectors,
            SearchOptions searchOptions,
            ConcurrentModificationPolicy cmPolicy
    ) {
        AbstractVectorIndex index;
        VectorFloat<?> searchVectorFloat;

        if (vectors instanceof VectorValues.SingleVectorValues singleVectorValues) {
            if (vectorIndexes.isMultiIndex()) {
                throw new IllegalArgumentException("Index must be selected for collection with more than 1 index");
            }
            // search by the only index
            index = vectorIndexes.getSingleIndex();
            searchVectorFloat = convertToOnHeapArrayVectorFloat(singleVectorValues.vector());
        } else if (vectors instanceof VectorValues.MultiIndexVectorValues multiIndexVectorValues
                && multiIndexVectorValues.indexNameToVector().size() == 1) {
            // search by named/non-default vector
            var entry = multiIndexVectorValues.indexNameToVector().entrySet().iterator().next();
            String indexName = entry.getKey();
            index = vectorIndexes.getIndex(indexName);
            if (index == null) {
                throw new IllegalArgumentException("No vector index named '" + indexName + "' is defined");
            }
            searchVectorFloat = convertToOnHeapArrayVectorFloat(entry.getValue());
        } else {
            throw new UnsupportedOperationException("Cannot search multiple vector indexes");
        }

        int limit = getPartitionLimit(searchOptions);
        int rerank = getEfSearch(index, searchOptions, limit);

        // capture logical timestamps before search for validation later
        int searchLogicalTime = getSearchLogicalTime();
        int searchIndexLogicalTime = index.getSearchLogicalTime();

        var results = index.search(searchVectorFloat, limit, rerank, cmPolicy);

        if (searchOptions.isIncludeValue()) {
            for (var it = results.results(); it.hasNext(); ) {
                fillValue(it, cmPolicy, searchLogicalTime);
            }
        }
        if (searchOptions.isIncludeVectors()) {
            for (var it = results.results(); it.hasNext(); ) {
                fillVector(index, it, cmPolicy);
            }

            // do the check late to avoid overhead for each vector in common case
            if (index.getSearchLogicalTime() != searchIndexLogicalTime) {
                // we cannot reliably get vectors
                // if the policy does not throw, this is last attempt
                // in such case return empty result, which at least is formally correct
                // maybe other partitions will be more lucky
                for (var it = results.results(); it.hasNext(); it.next()) {
                    cmPolicy.action(it);
                }
            }
        }
        return results;
    }

    public String validateAndGetIndexName(String indexName) {
        return vectorIndexes.validateAndGetIndex(indexName).indexName;
    }

    public void lockIndexMutation(String indexName, UUID uuid) {
        vectorIndexes.validateAndGetIndex(indexName).lockIndexMutation(uuid);
    }

    public void unlockIndexMutation(String indexName) {
        vectorIndexes.validateAndGetIndex(indexName).unlockIndexMutation();
    }

    private void checkMutatingOperationAllowed() {
        vectorIndexes.forEachIndex(AbstractVectorIndex::checkMutatingOperationAllowed);
    }

    /**
     * Finds any locked index (if any). Index locks are not taken in order.
     * With continuous optimisation requests it might be possible to starve
     * mutations on collection with >1 index, but this is unlikely.
     * <p>
     * Should be invoked only on partition thread to guarantee stability of the
     * result as defined below.
     * <p>
     * Note: Index is locked on partition thread but unlocked on arbitrary thread.
     * If this method returns {@link #NOT_LOCKED} this will not change until
     * another operation enters the partition thread. However, in the other direction
     * it is not guaranteed: if this method reported locked index, in a moment
     * it can return a different index or {@link #NOT_LOCKED}.
     *
     * @return name of locked index (can be null for unnamed index) if any
     *         or {@link #NOT_LOCKED} otherwise
     */
    public Object findLockedIndex() {
        var lockedIndex = vectorIndexes.findLockedIndex();
        return lockedIndex != null ? lockedIndex.indexName : NOT_LOCKED;
    }

    public boolean isIndexLocked(String indexName) {
        return !vectorIndexes.validateAndGetIndex(indexName).isMutatingOperationAllowed();
    }

    public void clear() {
        checkMutatingOperationAllowed();
        vectorIndexes.clear();
        recordStore.clear();
    }

    public long size() {
        return recordStore.size();
    }

    public boolean isEmpty() {
        return recordStore.isEmpty();
    }

    public OnDemandStats getOnDemandStats() {
        return new OnDemandStatsImpl(size(), heapBytesUsed());
    }

    public VectorIndexStats getVectorIndexStats() {
        return vectorIndexes.getVectorIndexStats();
    }

    private int getPartitionLimit(SearchOptions searchOptions) {
        int resultLimit = searchOptions.getLimit();
        Integer maybePartitionLimit = PARTITION_LIMIT.get(searchOptions);
        if (maybePartitionLimit == null) {
            return resultLimit;
        }
        if (maybePartitionLimit < 0) {
            throw new IllegalArgumentException("Partition limit cannot be negative");
        }
        if (maybePartitionLimit > resultLimit) {
            throw new IllegalArgumentException("Number of neighbours requested from partition "
                    + "is greater than requested result size");
        }
        if (maybePartitionLimit * nodeEngine.getPartitionService().getPartitionCount() < resultLimit) {
            // this check may be problematic if we execute searches only on selected partitions
            // (PartitionPredicate) or if the users knows that the distribution of data is skewed
            // and only some partitions contain data. In current form it prevents a likely mistake.
            throw new IllegalArgumentException("Number of neighbours requested from partition "
                    + "is not sufficient to generate full requested result");
        }
        return maybePartitionLimit;
    }

    /**
     * Determines efSearch (rerankK in JVector API) for given search based on hints and simple heuristics
     *
     * @param index         index for which search will be executed
     * @param searchOptions search option
     * @param limit         topK for the search
     * @return rerankK value for search
     */
    private int getEfSearch(AbstractVectorIndex index, SearchOptions searchOptions, int limit) {
        Integer maybeEfSearch = EF_SEARCH.get(searchOptions);
        if (maybeEfSearch == null) {
            // initially use a conservative value, the same as in previous version.
            // better heuristics will be implemented here.
            return limit;
        }
        if (maybeEfSearch < limit) {
            throw new IllegalArgumentException(
                    String.format("efSearch (%d) is smaller than the number of neighbours requested from partition (%d). "
                            + "If you specified %s hint, it has to have value at least %d. "
                            + "If you specified both %s and %s hints, they have to meet the requirements.",
                            maybeEfSearch, limit,
                            EF_SEARCH.name(), limit,
                            EF_SEARCH.name(), PARTITION_LIMIT.name()));
        }
        return maybeEfSearch;
    }

    private void fillValue(Iterator<? extends SearchResult<Data, Data>> it,
                           ConcurrentModificationPolicy cmPolicy,
                           int searchLogicalTime) {
        var result = (InternalSearchResult<Data, Data>) it.next();
        Data value = getInternal(result.getKey(), searchLogicalTime);
        // Value may be missing if the entry was removed or updated just after it was found
        if (value == null) {
            // we rely on the fact that search results contain mutable iterator,
            // which is not guaranteed by public contract.
            cmPolicy.action(it);
        } else {
            result.setValue(value);
        }
    }

    private void fillVector(AbstractVectorIndex index,
                                   Iterator<? extends SearchResult<?, ?>> it,
                                   ConcurrentModificationPolicy cmPolicy) {
        var result = (InternalSearchResult<?, ?>) it.next();
        // return vector from index used in the query
        VectorFloat<?> vector = index.vectorsSupplier.getVector(result.id());
        // Vector may be missing if during this search the node was deleted and the cleanup was performed.
        if (vector == null) {
            // we rely on the fact that search results contain mutable iterator,
            // which is not guaranteed by public contract.
            cmPolicy.action(it);
        } else {
            var singleVectorValues = vectorConverter.createSingleVectorValue(vector);
            result.setVectors(singleVectorValues);
        }
    }

    //region migration support

    VectorIndexHolder getVectorIndexes() {
        return vectorIndexes;
    }

    Storage<Data, Record<Data>> getRecordStore() {
        return recordStore;
    }

    /**
     * Prepares the collection partition for migration. The main activity is to optimize indexes
     * which is necessary before serialization. It also removes deleted entries and
     * reduces size of data to be sent.
     */
    public void prepareForMigration() {
        vectorIndexes.forEachIndex(index -> index.prepareForMigration(this, nodeEngine));
    }

    /**
     * Restores state of the partition from replicated data
     */
    void resetState(ReplicationStateHolder.CollectionReplicationStateHolder state) {
        if (!state.config.equals(this.config)) {
            // This might happen in case when static config on members is inconsistent.
            // It is not possible to correctly migrate vector index with mismatched config.
            throw new IllegalArgumentException("Incoming vector collection partition has different configuration");
        }

        if (state.indexes.size() != vectorIndexes.getSize()) {
            throw new IllegalStateException("Number of configured indexes (" + vectorIndexes.getSize()
                    + ") is not equal to number of received indexes (" + state.indexes.size() + ")");
        }

        // KV store
        assert recordStore.isEmpty() : "Migration to non-empty storage";
        state.entries.forEach((key, value) -> recordStore.put(key, createRecord(value)));

        // indexes
        state.indexes.forEach(indexState -> vectorIndexes.validateAndGetIndex(indexState.getName()).resetState(indexState));
    }

    //endregion

    /**
     * A holder for managing a collection of vector indexes.
     */
    static class VectorIndexHolder implements Measurable {

        private static final long FIXED_HEAP_BYTES_USED = OBJECT_HEADER_SIZE + 3L * REFERENCE_COST_IN_BYTES + Integer.BYTES;

        private final Map<String, AbstractVectorIndex> vectorIndexMap;
        private final int size;
        private final String singleIndexName;
        private final List<VectorIndexConfig> indexConfigs;
        private final VectorIndexFactory vectorIndexFactory;

        private VectorIndexHolder(List<VectorIndexConfig> indexConfigs, VectorIndexFactory vectorIndexFactory) {
            this.indexConfigs = indexConfigs;
            this.size = indexConfigs.size();
            this.singleIndexName = size == 1 ? indexConfigs.get(0).getName() : null;
            this.vectorIndexMap = new HashMap<>();
            this.vectorIndexFactory = vectorIndexFactory;
            initIndexMap(indexConfigs);
        }

        private void initIndexMap(List<VectorIndexConfig> indexConfigs) {
            for (var indexConfig : indexConfigs) {
                vectorIndexMap.put(
                        indexConfig.getName(),
                        vectorIndexFactory.create(indexConfig)
                );
            }
        }

        public int getSize() {
            return size;
        }

        /**
         * Checks if the holder manages multiple vector indexes.
         *
         * @return True if there are multiple indexes, false otherwise.
         */
        public boolean isMultiIndex() {
            return size > 1;
        }

        public boolean doesIndexExist(String indexName) {
            return vectorIndexMap.containsKey(indexName);
        }

        public void forEachIndex(Consumer<AbstractVectorIndex> action) {
            vectorIndexMap.values().forEach(action);
        }

        public AbstractVectorIndex findLockedIndex() {
            return vectorIndexMap.values().stream()
                    .filter(Predicate.not(AbstractVectorIndex::isMutatingOperationAllowed))
                    .findAny()
                    .orElse(null);
        }

        /**
         * Retrieves the vector index with the specified name.
         * Does not perform any validation on the index name.
         *
         * @param indexName The name of the index to retrieve.
         * @return The vector index with the specified name, or null if it does not exist.
         */
        public AbstractVectorIndex getIndex(String indexName) {
            assert indexName != null : "the index name has not been specified.";
            return vectorIndexMap.get(indexName);
        }

        /**
         * Retrieves the single vector index managed by this holder.
         * Should only be called if there is exactly one index.
         *
         * @return The single vector index.
         */
        public AbstractVectorIndex getSingleIndex() {
            assert !isMultiIndex() : "not permitted for use with a multi-index holder.";
            return vectorIndexMap.get(singleIndexName);
        }

        /**
         * Validates the index name and retrieves the corresponding vector index.
         * Throws an exception if the index name is not specified for a multi-index collection or if the index is not found.
         *
         * @param indexName The name of the index to validate and retrieve.
         * @return The validated vector index.
         * @throws IllegalArgumentException if the index name is not specified or not found.
         */
        public AbstractVectorIndex validateAndGetIndex(String indexName) {
            if (isMultiIndex()) {
                if (indexName == null) {
                    throw new IllegalArgumentException("The index name has not been specified.");
                }
                AbstractVectorIndex vectorIndex = vectorIndexMap.get(indexName);
                if (vectorIndex == null) {
                    throw new IllegalArgumentException("No index was found with the name: " + indexName);
                }
                return vectorIndex;
            } else {
                if (indexName != null && !indexName.equals(singleIndexName)) {
                    throw new IllegalArgumentException("No index was found with the name: " + indexName);
                }
                return getSingleIndex();
            }
        }

        public void clear() {
            initIndexMap(indexConfigs);
        }

        @Override
        public long heapBytesUsed() {
            long heapBytesUsed = FIXED_HEAP_BYTES_USED + (long) vectorIndexMap.size() * REFERENCE_COST_IN_BYTES;
            for (AbstractVectorIndex index : vectorIndexMap.values()) {
                heapBytesUsed += index.heapBytesUsed();
            }
            return heapBytesUsed;
        }

        public VectorIndexStats getVectorIndexStats() {
            if (isMultiIndex()) {
                VectorIndexStatsImpl stats = new VectorIndexStatsImpl();
                forEachIndex(index -> stats.add(index.getStats()));
                return stats;
            } else {
                return getSingleIndex().getStats();
            }
        }
    }

    @Override
    public long heapBytesUsed() {
        return FIXED_HEAP_BYTES_USED + vectorIndexes.heapBytesUsed()
                + recordStore.getEntryCostEstimator().getEstimate();
    }

    /**
     * Apply the given {@code consumer} sequentially to all entries in this storage for the purpose of split-brain merge.
     */
    public void consumeAll(BiConsumer<Integer, VectorCollectionMergeTypes<Object, VectorDocument<?>>> consumer) {
        var iterator = recordStore.mutationTolerantIterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            var mergingEntry = new VectorCollectionMergingEntryImpl(nodeEngine.getSerializationService(), entry.getKey(),
                                                            entry.getValue().getValue(), getVectorValues(entry.getKey()));
            consumer.accept(partitionId, mergingEntry);
        }
    }

    /**
     * Merge the given {@code mergingEntry} to the current contents in this storage instance according to {@code policy} merge
     * policy.
     * @param mergingEntry  the entry to consider for merging in this storage
     * @param policy        the merge policy to use for determining the merged {@code VectorDocument}
     */
    public MergeResponse merge(VectorCollectionMergeTypes<Object, VectorDocument<?>> mergingEntry,
                              SplitBrainMergePolicy<VectorDocument<?>,
                              VectorCollectionMergeTypes<Object, VectorDocument<?>>, Object> policy) {
        Data dataKey = (Data) mergingEntry.getRawKey();
        var vectorDocument = get(dataKey);
        var existingMergingEntry = vectorDocument == null ? null
                : new VectorCollectionMergingEntryImpl(nodeEngine.getSerializationService(), dataKey, vectorDocument);
        var mergedDocument = (VectorDocument<Object>) policy.merge(mergingEntry, existingMergingEntry);
        if (mergedDocument != null) {
            if (existingMergingEntry != null && mergedDocument.equals(vectorDocument)) {
                return MergeResponse.NO_MERGE_APPLIED;
            } else {
                set(dataKey, nodeEngine.toData(mergedDocument.getValue()), mergedDocument.getVectors());
                return new MergeResponse(MergeStatus.ENTRY_UPDATED, mergedDocument);
            }
        } else if (existingMergingEntry != null) {
            // if merge policy returned null and an entry exists for that key, then the entry should be removed
            delete(dataKey);
            return MergeResponse.ENTRY_REMOVED;
        }
        return MergeResponse.NO_MERGE_APPLIED;
    }

    public enum MergeStatus {
        NO_MERGE_APPLIED,
        ENTRY_REMOVED,
        ENTRY_UPDATED
    }

    public record MergeResponse(MergeStatus status, VectorDocument<?> mergedValue) {
        // constants when merged value is not applicable
        public static final MergeResponse NO_MERGE_APPLIED = new MergeResponse(MergeStatus.NO_MERGE_APPLIED, null);
        public static final MergeResponse ENTRY_REMOVED = new MergeResponse(MergeStatus.ENTRY_REMOVED, null);
    }

    public static final class VectorCollectionMergingEntryImpl<K, V extends VectorDocument<?>>
            implements VectorCollectionMergeTypes<K, V>, IdentifiedDataSerializable, SerializationServiceAware {

        private transient SerializationService serializationService;
        private Data key;
        private VectorDocument<Data> vectorDocument;

        public VectorCollectionMergingEntryImpl() {
        }

        public VectorCollectionMergingEntryImpl(SerializationService serializationService,
                                                Data key, VectorDocument<Data> vectorDocument) {
            this.serializationService = serializationService;
            this.key = key;
            this.vectorDocument = vectorDocument;
        }

        public VectorCollectionMergingEntryImpl(SerializationService serializationService,
                                                Data key, Data value, VectorValues vectorValues) {
            this.serializationService = serializationService;
            this.key = key;
            this.vectorDocument = new DataVectorDocument(value, vectorValues);
        }

        @Override
        public K getKey() {
            return serializationService.toObject(key);
        }

        @Override
        public Data getRawKey() {
            return key;
        }

        @Override
        public V getDeserializedValue() {
            return (V) VectorUtil.deserialize(vectorDocument, serializationService);
        }

        @Override
        public VectorDocument<Data> getRawValue() {
            return vectorDocument;
        }

        @Override
        public int getFactoryId() {
            return VectorCollectionSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return VectorCollectionSerializerHook.VECTOR_COLLECTION_MERGING_ENTRY;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            IOUtil.writeData(out, key);
            out.writeObject(vectorDocument);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            key = IOUtil.readData(in);
            vectorDocument = in.readObject();
        }

        @Override
        public void setSerializationService(SerializationService serializationService) {
            this.serializationService = serializationService;
        }
    }
}
