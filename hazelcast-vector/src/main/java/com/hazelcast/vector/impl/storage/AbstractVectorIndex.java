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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.memory.Measurable;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.vector.IndexMutationDisallowedException;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.impl.stats.VectorIndexStats;
import com.hazelcast.vector.impl.stats.VectorIndexStatsImpl;
import com.hazelcast.vector.impl.storage.graph.HazelcastBuildScoreProvider;
import com.hazelcast.vector.impl.storage.graph.HazelcastBuiltinVectorSimilarityFunction;
import com.hazelcast.vector.impl.storage.graph.HazelcastVectorSimilarityFunction;
import io.github.jbellis.jvector.annotations.VisibleForTesting;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.HazelcastGraphIndexViewProvider;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.util.BitSet;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.JVMUtil.OBJECT_HEADER_SIZE;
import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;

/**
 * Abstract onheap vector index implementation.
 * <p>
 * Index as a whole consists of the following elements:
 * <ul>
 *     <li>Vector storage ({@link #vectorsSupplier}: nodeId -> float[]</li>
 *     <li>JVector graph index builder which gets all updates</li>
 *     <li>JVector graph searcher</li>
 * </ul>
 * <p>
 * This class and subclasses are thread-safe for many concurrent searches and 1 mutating operation
 * (including {@link #cleanup()}). In some cases conflicts are resolved using {@link ConcurrentModificationPolicy}.
 */
@SuppressWarnings("checkstyle:magicnumber")
public abstract class AbstractVectorIndex implements Measurable {

    static final long FIXED_HEAP_BYTES_USED = OBJECT_HEADER_SIZE + 7L * REFERENCE_COST_IN_BYTES
        // idGenerator, indexVersion, maxDegree, efConstruction, dimensions
        + 5 * Integer.BYTES
        // indexOptimizationFuture (assuming null value held)
        + OBJECT_HEADER_SIZE + REFERENCE_COST_IN_BYTES
        // stats
        + VectorIndexStatsImpl.FIXED_HEAP_BYTES_USED;

    /**
     * Each Vector Index maintains separate key <=> id <=> vector mapping.
     * For collections with multiple indexes and vector deduplication each index can have
     * different duplicates.
     * @implNote should be accessed only on partition thread for updates and serialization of replication operations
     * <p>
     * todo need proper ID mapper impl here, in particular with ability to reuse freed ids
     *  however such reuse can be problematic for concurrent searches (see rationale for indexVersion)
     */
    protected int idGenerator;

    /**
     * Maintains the graph node ID -> float[] vector mapping
     */
    volatile UpdatableVectorsSource vectorsSupplier;

    @VisibleForTesting
    GraphIndexBuilder indexBuilder;

    final String indexName;
    /**
     * maximum number of connections per node
     */
    private final int maxDegree;
    /**
     * size of the beam search to use when finding nearest neighbors
     */
    private final int efConstruction;
    /**
     * number of vector dimensions
     */
    private final int dimensions;

    private final HazelcastVectorSimilarityFunction similarityFunction;

    /**
     * {@link GraphSearcher} cannot be used concurrently. Create a small 1-element pool of searchers.
     * In case when the number of partitions on member is significantly larger than
     * number of search threads it should provide reasonable balance between performance and
     * complexity.
     */
    private volatile SearcherPool searcherPool;

    /**
     * When {@link #idGenerator} is reset, nodeIds obtained by concurrent searches become invalid.
     * However, they might accidentally resolve as new, unrelated vectors if new vectors are inserted.
     * To prevent such situation, search should capture snapshot of the index version at the beginning
     * and compare it with value after search. If they differ, the vectors can no longer be obtained.
     * <p>
     * This field is ever updated only in rare cases when index becomes empty.
     * It is updated only on partition thread but read by multiple threads.
     */
    private volatile int indexVersion;

    /**
     * Current/most recent optimization state.
     * Null value means that there is no optimization in progress - mutations are allowed.
     */
    private final AtomicReference<OptimizationState> indexOptimizationState = new AtomicReference<>();

    private final VectorIndexStatsImpl stats = new VectorIndexStatsImpl();

    /**
     * @param uuid optimization request UUID
     * @param indexOptimizationFuture Future allowing to wait for the optimization to finish
     */
    private record OptimizationState (@Nonnull UUID uuid, @Nonnull InternalCompletableFuture<Void> indexOptimizationFuture) {
        OptimizationState(@Nonnull UUID uuid) {
            this(Objects.requireNonNull(uuid), new InternalCompletableFuture<>());
        }

        boolean isDone() {
            return indexOptimizationFuture.isDone();
        }

        void complete() {
            indexOptimizationFuture.complete(null);
        }

        void completeExceptionally(Exception e) {
            indexOptimizationFuture.completeExceptionally(e);
        }

        void join() {
            indexOptimizationFuture.joinInternal();
        }
    }

    private enum CleanupMode { FULL, DELETED_NODES }

    AbstractVectorIndex(
            String indexName,
            Metric metric,
            int maxDegree,
            int efConstruction,
            int dimensions
    ) {
        this.indexName = indexName;
        this.maxDegree = maxDegree;
        this.efConstruction = efConstruction;
        this.dimensions = dimensions;
        this.similarityFunction = asVectorSimilarityFunction(metric);
        vectorsSupplier = new UpdatableVectorsSource(dimensions);
        indexBuilder = createIndexBuilder(vectorsSupplier);
        searcherPool = createSearcherPool(indexBuilder);
    }

    protected GraphIndexBuilder createIndexBuilder(RandomAccessVectorValues vectorsSource) {
        // TODO: It would be better to create different BuildScoreProvider for different similarity function
        //  because the current implementation still calculates the centroid for cosine similarity incorrectly.
        return new GraphIndexBuilder(
                new HazelcastBuildScoreProvider(vectorsSource, similarityFunction),
                vectorsSource.dimension(),
                maxDegree,
                efConstruction,
                1.2f,
                1.4f
        );
    }

    private SearcherPool createSearcherPool(GraphIndexBuilder indexBuilder) {
        return new SearcherPool(indexBuilder.getGraph());
    }

    private HazelcastVectorSimilarityFunction asVectorSimilarityFunction(Metric m) {
        return switch (m) {
            case DOT -> HazelcastBuiltinVectorSimilarityFunction.DOT_PRODUCT;
            case COSINE -> HazelcastBuiltinVectorSimilarityFunction.cosine();
            case EUCLIDEAN -> HazelcastBuiltinVectorSimilarityFunction.EUCLIDEAN;
        };
    }

    public int getEfConstruction() {
        return efConstruction;
    }

    /**
     * @param returnPrevious if the caller is interested in previous vector associated with the key
     * @return vector previously associated with given key or null if the key is new or returnPrevious is false
     */
    public VectorFloat<?> put(Data key, VectorFloat<?> vector, boolean returnPrevious) {
        checkMutatingOperationAllowed();
        validateVector(vector);

        VectorFloat<?> previousVector = putInternal(key, vector);
        if (returnPrevious && previousVector != null) {
            // this relies on the fact that deleteNode does not physically delete/destroy the vector.
            // previousVectorId is valid until cleanup which cannot happen concurrently with put.
            return previousVector;
        }
        return null;
    }

    public void put(Data key, VectorFloat<?> vector) {
        checkMutatingOperationAllowed();
        validateVector(vector);
        putInternal(key, vector);
    }

    public VectorFloat<?> get(Data key) {
        var nodeId = getNodeIdByKey(key);
        if (nodeId == null) {
            throw new IllegalStateException("The entry with the requested key does not exist.");
        }
        return vectorsSupplier.getVector(nodeId);
    }

    public boolean delete(Data key) {
        checkMutatingOperationAllowed();
        return deleteInternal(key);
    }

    public void cleanup() {
        var optimizationState = indexOptimizationState.get();
        if (optimizationState == null) {
            throw new IllegalStateException("The index must be locked for mutation before the optimization process begins.");
        }
        if (optimizationState.isDone()) {
            throw new IllegalStateException("Optimization already done, lock is not reusable");
        }

        try {
            cleanupInternal(CleanupMode.FULL);
            optimizationState.complete();
        } catch (Exception e) {
            optimizationState.completeExceptionally(e);
        }
    }

    private void cleanupInternal(CleanupMode mode) {
        // Removed nodes are utilized during graph search.
        // Therefore, they should be removed from the graph before being deleted from the supplier.
        // On the other hand, removedNode will be deleted after cleaning,
        // so we retain them to clean up the supplier after cleaning up the index.
        List<Integer> candidateToBeRemoved = new ArrayList<>();
        var deletedNodes = getRemovedNodes();
        for (int i = deletedNodes.nextSetBit(0); i != Integer.MAX_VALUE; i = deletedNodes.nextSetBit(i + 1)) {
            candidateToBeRemoved.add(i);
        }
        switch (mode) {
            case DELETED_NODES -> indexBuilder.removeDeletedNodes();
            default -> indexBuilder.cleanup();
        }
        candidateToBeRemoved.forEach(vectorsSupplier::remove);
    }

    int getSearchLogicalTime() {
        return indexVersion;
    }

    // visible for tests
    SearchResults<Data, Data> search(VectorFloat<?> vector, int topK) {
        return search(vector, topK, topK, ConcurrentModificationPolicy.THROW);
    }

    public SearchResults<Data, Data> search(VectorFloat<?> vector, int topK, int rerankK, ConcurrentModificationPolicy cmPolicy) {
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be > 0");
        }

        SearchScoreProvider scoreProvider = createSearchScoreProvider(
                vector,
                cmPolicy
        );

        SearchResult searchResult;
        SearcherPool searcherPoolRef = searcherPool;
        var searcher = searcherPoolRef.acquire();
        try {
            searchResult = searcher.search(scoreProvider, topK, rerankK, 0f, 0f, Bits.ALL);
        } finally {
            searcherPoolRef.release(searcher);
        }
        // each retry is treated as a new query to have consistent index-level stats
        stats.incrementQueryCount();
        stats.addVisitedNodes(searchResult.getVisitedCount());

        return toDataSearchResults(searchResult, topK, cmPolicy);
    }

    private SearchScoreProvider createSearchScoreProvider(VectorFloat<?> vector, ConcurrentModificationPolicy cmPolicy) {
            var sf = new ScoreFunction.ExactScoreFunction() {
                @Override
                public float similarityTo(int node2) {
                    var vector2 = vectorsSupplier.getVector(node2);
                    if (vector2 == null) {
                        // If optimization was performed with exactly (un)lucky timing with regard to search,
                        // we may get node2 that was just removed from vectorsSupplier by optimization.
                        cmPolicy.action();

                        // If we did not throw (last retry), do not fail the search but return worst possible similarity
                        // (prefer availability). With such bad similarity node should not be included in results anyway,
                        // however, if it is (e.g. due to too few other candidates), the search will fail later.
                        // This can also cause skipping parts of the graph during search, due to bad score of intermediate node
                        // or in extreme cases generating fewer results than expected.
                        return Float.NEGATIVE_INFINITY;
                    }
                    return similarityFunction.compare(vector, vector2);
                }
            };
            return new SearchScoreProvider(sf);
    }

    /**
     * Attempts to lock the index for mutation.
     * <p>
     * Usage of this method ensures that no index mutations can occur while an index
     * optimization is in progress.
     * </p>
     *
     * @param uuid optimization request UUID - informative only
     * @throws IndexMutationDisallowedException if the index optimization process is currently in progress.
     *
     * @apiNote this method should usually be called on partition thread to guarantee
     * that no mutating operations are allowed concurrently. However, it may be also
     * called from other threads if there is a different way to guarantee that
     * (e.g. partition is marked as migrating).
     */
    public void lockIndexMutation(UUID uuid) {
        // locking must be an atomic operation, because readers of the flag can be in other threads
        OptimizationState currentOptimization = indexOptimizationState.compareAndExchange(null, new OptimizationState(uuid));
        if (currentOptimization != null) {
            throw new IndexMutationDisallowedException("Index optimization process with uuid = "
                    + currentOptimization.uuid() + " is in progress.");
        }
        // successfully locked
    }

    /**
     * Unlocks the index for mutation.
     *
     * @throws IllegalStateException if the index is not locked for mutation.
     */
    public void unlockIndexMutation() {
        var currentState = indexOptimizationState.get();
        if (currentState != null && !currentState.isDone()) {
            // there can be a retry waiting on that future, so terminate it in case something bad happened
            currentState.completeExceptionally(new HazelcastException("Index optimization terminated by unlocking"));
        }
        if (currentState == null || !indexOptimizationState.compareAndSet(currentState, null)) {
            throw new IllegalStateException("Failed to unlock a collection that was not locked.");
        }
    }

    public boolean isMutatingOperationAllowed() {
        return indexOptimizationState.get() == null;
    }

    /**
     * Checks if a mutating operation on the index is allowed.
     *
     * @throws IndexMutationDisallowedException if an index optimization process is currently in progress.
     */
    public void checkMutatingOperationAllowed() {
        if (!isMutatingOperationAllowed()) {
            throw new IndexMutationDisallowedException("Index optimization process is in progress.");
        }
    }

    public VectorIndexStats getStats() {
        return stats;
    }

    protected BitSet getRemovedNodes() {
        return indexBuilder.getGraph().getDeletedNodes();
    }

    protected abstract boolean deleteInternal(Data key);

    /**
     * @return previous vector associated with given key, if any
     */
    protected abstract VectorFloat<?> putInternal(Data key, VectorFloat<?> vector);

    protected abstract SearchResults<Data, Data> toDataSearchResults(SearchResult searchResult, int limit,
                                                         ConcurrentModificationPolicy cmPolicy);

    protected abstract Integer getNodeIdByKey(Data key);

    /**
     * Determines if there are live nodes in the index.
     * Live nodes are those that have not been removed.
     *
     * @return {@code true} if live nodes are present, {@code false} otherwise.
     */
    protected abstract boolean hasLiveNodes();

    /**
     * @return if the id generator was reset
     */
    // this is the only place that updates indexVersion and is called only from partition thread
    @SuppressWarnings({"NonAtomicOperationOnVolatileField", "squid:S3078"})
    protected boolean maybeRecreateIndex() {
        if (hasLiveNodes()) {
            return false;
        }
        // after removing all nodes from the graph,
        // it becomes broken, cleanup operation fixes the graph
        cleanupInternal(CleanupMode.FULL);
        // reset id generator to avoid starting with big ids, allocating large bit sets
        // and doing lots of work during cleanup
        idGenerator = 0;
        // invalidate any searches that might want vectors from previous cycle of ids
        indexVersion++;

        return true;
    }

    private void validateVector(VectorFloat<?> vector) {
        validateDimension(vector.length());
    }

    void validateVector(float[] vector) {
        validateDimension(vector.length);
    }

    private void validateDimension(int length) {
        if (length != dimensions) {
            throw new IllegalArgumentException("Vector length " + length
                    + " different than expected for index " + indexName);
        }
    }

    private static class SearcherPool {

        private final AtomicReference<GraphSearcher> pool;

        private final Supplier<GraphSearcher> graphSearcherSupplier;

        SearcherPool(OnHeapGraphIndex graph) {
            this.graphSearcherSupplier = () -> new GraphSearcher(new HazelcastGraphIndexViewProvider(graph));
            this.pool = new AtomicReference<>(graphSearcherSupplier.get());
        }

        public GraphSearcher acquire() {
            // try to use cached, if busy create new
            var pooledSearcher = pool.getAndSet(null);
            return pooledSearcher != null ? pooledSearcher : graphSearcherSupplier.get();
        }

        public void release(GraphSearcher searcher) {
            pool.compareAndSet(null, searcher);
        }
    }

    /**
     * Calculates an approximation of heap bytes used by this index. It does not take into account bytes occupied by:
     * <ul>
     *     <li>{@code GraphSearcher} heap usage</li>
     *     <li>{@code GraphIndexBuilder} fields like temporary node arrays used while adding nodes to the graph,
     *          other than the heap used by the {@code OnHeapGraphIndex} itself</li>
     * </ul>
     */
    @Override
    public long heapBytesUsed() {
        return FIXED_HEAP_BYTES_USED + vectorsSupplier.heapBytesUsed() + indexBuilder.getGraph().ramBytesUsed();
    }

    //region migration support

    void prepareForMigration(VectorCollectionStorage vectorCollectionStorage, NodeEngine nodeEngine) {
        // There is no race condition with optimization initiation, as the partition has already been marked as migrating.
        assert !ThreadUtil.isRunningOnPartitionThread() : "Preparation should be offloaded";
        assert nodeEngine.getPartitionService().getPartition(vectorCollectionStorage.getPartitionId()).isMigrating()
                : "Partition should be marked as migrating";

        var optimizationState = indexOptimizationState.get();
        if (optimizationState != null) {
            ILogger logger = nodeEngine.getLogger(AbstractVectorIndex.class);
            logger.info("Index optimization in progress for partitionId=" + vectorCollectionStorage.getPartitionId()
                    + " index name=" + indexName
                    + ", waiting for completion before continuing migration");

            // There is an ongoing optimization that started before migration.
            // Need to wait for it to end, no new mutations should be executed after the optimization had started
            // and the partition is now also marked as migrating, so there are 2 reasons to reject mutations.
            // It is sufficient to wait for optimization (index cannot be serialized during optimization)
            // and there is no need to start yet another optimization.
            optimizationState.join();
            return;
        }

        // We do not need to lockIndexMutation() here, the partition has already been marked as migrating earlier
        // and no new mutations (including optimization) are allowed.
        // For migration it is sufficient that no deleted nodes exist, other optimizations (connectivity, max degree
        // enforcement) and not necessary.
        cleanupInternal(CleanupMode.DELETED_NODES);
    }

    abstract void writeKeyToNodeIdMapping(ObjectDataOutput out, Data key) throws IOException;

    void resetState(ReplicationStateHolder.CollectionReplicationStateHolder.IndexReplicationStateHolder indexState) {
        checkMutatingOperationAllowed();

        idGenerator = indexState.idGeneratorState;

        // order of publishing of objects is the same as in maybeRecreateIndex
        indexBuilder = indexState.index.indexBuilder;
        searcherPool = createSearcherPool(indexBuilder);
        vectorsSupplier = indexState.vectorsSupplier;
    }

    //endregion
}
