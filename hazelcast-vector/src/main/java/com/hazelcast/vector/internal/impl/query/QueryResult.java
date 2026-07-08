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

package com.hazelcast.vector.internal.impl.query;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchResult;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.internal.impl.VectorCollectionSerializerHook;
import io.github.jbellis.jvector.graph.NodeQueue;
import io.github.jbellis.jvector.util.AbstractLongHeap;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.IntFunction;

/**
 * Aggregates vector search results from multiple sources (partitions, members).
 * Sources are assigned consecutive integer ids in range {@code [0 .. maxSourcesCount)}.
 * This class does not require results from all possible sources to be actually present.
 * When requested it will aggregate only the existing results.
 * <p>
 * The class is generally not thread-safe. Only for selected operations concurrent invocations are allowed.
 */
public class QueryResult {
    private static final SourceResult EMPTY = new SourceResult(null, null);

    private final int maxSourcesCount;
    private final Int2ObjectHashMap<SourceResult> sourceResults;
    // source ids sorted by current best score.
    private final NodeQueue heap;
    // number of requested results
    private final int limit;
    // total count of results from all sources
    private int size;

    /**
     * @param maxSourcesCount maximum number of result sources
     * @param expectedCount number of expected result sources
     * @param limit number of search results to return
     */
    public QueryResult(int maxSourcesCount, int expectedCount, int limit) {
        this(maxSourcesCount, expectedCount, limit, FixedSizeLongHeap::new);
    }

    /**
     * @param maxSourcesCount maximum number of results
     * @param expectedCount number of expected result sources
     * @param limit number of search results to return
     * @param heapConstructor creates heap to use when aggregating the results
     */
    public QueryResult(int maxSourcesCount, int expectedCount, int limit,
                       IntFunction<AbstractLongHeap> heapConstructor) {
        assert maxSourcesCount > 0;
        assert limit > 0;

        this.maxSourcesCount = maxSourcesCount;
        this.limit = limit;

        sourceResults = new Int2ObjectHashMap<>(expectedCount);

        int heapSize = expectedCount;
        // in general, we do not have to keep more source results than limit,
        // but it would require different data structure than a heap.
        heap = new NodeQueue(heapConstructor.apply(heapSize), NodeQueue.Order.MAX_HEAP);
    }

    /**
     * Accumulates a new result.
     * <p>
     * This method is thread-safe.
     *
     * @param sourceId id of the result source
     * @param results results from the source
     */
    public synchronized void addResult(int sourceId, SearchResults<Data, Data> results) {
        assert sourceId >= 0 && sourceId < maxSourcesCount : "Source id out of range";
        assert !sourceResults.containsKey(sourceId) : "Duplicate result for source " + sourceId;

        if (results.size() > 0) {
            Iterator<? extends SearchResult<Data, Data>> resultsIterator = results.results();
            SearchResult<Data, Data> bestSourceResult = resultsIterator.next();
            heap.push(sourceId, bestSourceResult.getScore());
            sourceResults.put(sourceId, new SourceResult(bestSourceResult, resultsIterator));
            size += results.size();
        } else {
            // empty result is not interesting, but track it, so we know that we got a response
            sourceResults.put(sourceId, EMPTY);
        }
    }

    /**
     * Completes the results aggregation and returns merged results.
     * After this method {@link #addResult(int, SearchResults)} cannot be invoked anymore.
     *
     * @return lazily evaluated merged results
     * @implNote Keeps all individual results in memory. It allows only for one-time iteration.
     */
    public synchronized SearchResults<Data, Data> complete() {
        // TODO: maybe some other approach to merging the result will perform better.
        //  This initial implementation is reasonably simple, but probably can be optimized.
        //  It could be easier if we had a collection instead of iterator.
        //  This implementation may be sufficiently good for cases where limit << number of sources
        return new LazilyAggregatedSearchResults();
    }

    public PartitionIdSet getSourceIds() {
        // TODO: does it make sense to generate PartitionIdSet in addResult? PartitionIdSet is not always needed.
        return new PartitionIdSet(maxSourcesCount, sourceResults.keySet());
    }

    private static final class SourceResult {
        private SearchResult<Data, Data> best;
        private final Iterator<? extends SearchResult<Data, Data>> remaining;

        private SourceResult(SearchResult<Data, Data> best, Iterator<? extends SearchResult<Data, Data>> remaining) {
            this.best = best;
            this.remaining = remaining;
        }
    }

    public class LazilyAggregatedSearchResults implements SearchResults<Data, Data>, IdentifiedDataSerializable {

        @Override
        public int size() {
            return Math.min(size, limit);
        }

        @Override
        public Iterator<SearchResult<Data, Data>> results() {
            return new SearchResultIterator(size());
        }

        @Override
        public int getFactoryId() {
            return VectorCollectionSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            // masquerade as SearchResultImpl for the purpose of efficient serialization
            return VectorCollectionSerializerHook.SEARCH_RESULTS;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(size());
            Iterator<? extends SearchResult<Data, Data>> results = results();
            int counter = 0;
            while (results.hasNext()) {
                out.writeObject(results.next());
                counter++;
            }
            assert counter == size() : "Inconsistent results or modified during serialization";
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            throw new UnsupportedOperationException("This class in not supposed to be deserialized");
        }
    }

    private class SearchResultIterator implements Iterator<SearchResult<Data, Data>> {
        // remaining number of results that can be fetched from this iterator
        private int remainingToFetch;

        SearchResultIterator(int size) {
            this.remainingToFetch = size;
        }

        @Override
        public boolean hasNext() {
            return remainingToFetch > 0;
        }

        @Override
        public SearchResult<Data, Data> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            // find current best
            int bestSourceId = heap.pop();
            SourceResult bestSourceResult = sourceResults.get(bestSourceId);
            SearchResult<Data, Data> r = bestSourceResult.best;

            // advance
            --remainingToFetch;
            if (hasNext() && bestSourceResult.remaining.hasNext()) {
                // replace removed entry only if needed (iterator is not exhausted)
                var nextResult = bestSourceResult.remaining.next();
                bestSourceResult.best = nextResult;
                // Note that BoundedLongHeap has replaceTop method which should be more efficient
                // than pop() + push()
                heap.push(bestSourceId, nextResult.getScore());
            }

            // done
            return r;
        }
    }

    /**
     * Merge results from all received partitions. Assumes that all partitions have finished successfully,
     * does not do meaningful handling of exceptions contained in results.
     *
     * @param results {@link SearchResults} for partitions. Does not have to contain all partitions.
     * @param searchOptions Search options
     * @param partitionCount Cluster partition count (used for optimizing allocations)
     * @return Merged results. They are evaluated lazily and only single-pass is allowed.
     */
    public static SearchResults<Data, Data> reduce(
            Map<Integer, Object> results,
            SearchOptions searchOptions,
            int partitionCount
    ) {
        QueryResult allQueryResults = new QueryResult(partitionCount, results.size(), searchOptions.getLimit());
        results.forEach((key, value) -> allQueryResults.addResult(key, (SearchResults<Data, Data>) value));
        return allQueryResults.complete();
    }
}
