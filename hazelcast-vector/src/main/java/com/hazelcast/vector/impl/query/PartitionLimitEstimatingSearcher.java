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

package com.hazelcast.vector.impl.query;

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.collection.ReadOptimizedLruCache;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchOptionsBuilder;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.impl.Hints;
import org.apache.commons.statistics.distribution.BinomialDistribution;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.util.JVMUtil.OBJECT_HEADER_SIZE;
import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;

public class PartitionLimitEstimatingSearcher implements Searcher {
    public static final long FIXED_HEAP_BYTES_USED = OBJECT_HEADER_SIZE + 3L * REFERENCE_COST_IN_BYTES
            // partitionCount
            + Integer.BYTES
            // probabilities
            + 2 * Double.BYTES;

    /**
     * Acceptable probability that a good result will be omitted because
     * partitionLimit was too small. This translates to extra quality loss
     * on top of vector search itself giving approximate results.
     */
    private static final double PROBABILITY_OF_OMISSION = 1e-4;
    // assume that there will not be too many distinct topK values used
    private static final int CACHE_SIZE = 50;
    private static final int CACHE_THRESHOLD = CACHE_SIZE + Math.min(CACHE_SIZE / 10, 50);

    private final int partitionCount;
    private final ILogger logger;

    private final double pSuccess;
    private final double acceptableOmissionProbability1Partition;
    private final ReadOptimizedLruCache<Integer, Integer> partitionLimitCache =
            new ReadOptimizedLruCache<>(CACHE_SIZE, CACHE_THRESHOLD);

    private final Searcher delegate;

    public PartitionLimitEstimatingSearcher(@Nonnull NodeEngine nodeEngine, @Nonnull Searcher delegate) {
        this(nodeEngine.getLogger(PartitionLimitEstimatingSearcher.class),
                nodeEngine.getPartitionService().getPartitionCount(),
                delegate);
    }

    // visible for tests
    PartitionLimitEstimatingSearcher(@Nonnull ILogger logger, int partitionCount, @Nonnull Searcher delegate) {
        this.delegate = delegate;
        this.logger = logger;

        this.partitionCount = partitionCount;
        pSuccess = 1.0d / partitionCount;
        acceptableOmissionProbability1Partition = PROBABILITY_OF_OMISSION / partitionCount;
    }

    @Override
    public CompletableFuture<SearchResults<Data, Data>> search(String collectionName,
                                                               VectorValues vectors, SearchOptions options,
                                                               @Nullable ClientEndpoint endpoint) {

        SearchOptions effectiveOptions;

        // special case for partition count = 1, because for that case partitionLimit is always equal to search limit.
        // special case for topK = 1, because for that case partitionLimit is always equal to search limit (i.e. 1)
        if (partitionCount > 1 && options.getLimit() > 1 && !Hints.PARTITION_LIMIT.isPresent(options)) {
            // Calculate partition limit so that probability of losing a good result
            // because of partition limit being too small is below acceptable threshold.
            // We assume random, uniform distribution of vectors to partitions. For skewed distributions
            // the value has to be adjusted.
            //
            //region derivation
            // The derivation of the formula is as follows:
            //
            // 1. calculate probability that given, selected partition X overflows (P_1Overflow), i.e.
            // that there will be > partitionLimit results that should be included in the final
            // merged result from partition X.
            //
            // To do that consider Bernoulli trial where success means that a good result belongs to partition X.
            // With uniform vectors distribution, the probability is 1 / partitionCount.
            //
            // Overflow of partition X happens when there are > partitionLimit successes.
            // This translates to binomial distribution survival probability.
            //
            // 2. estimate probability that any partition overflow (P_AnyOverflow).
            //
            // These events are not independent, so we estimate upper bound instead by assuming that they are
            // (this is a conservative approximation):
            //
            // P_AnyOverflow <= partitionCount * P_1Overflow
            //
            // 3. derive formula for partitionLimit
            //
            // W want P_AnyOverflow < PROBABILITY_OF_OMISSION.
            // Combining that with formula from 2 produces sufficient conditions for that to happen:
            //
            // P_AnyOverflow <= partitionCount * P_1Overflow < PROBABILITY_OF_OMISSION
            //
            // from which we get
            //
            // P_1Overflow < PROBABILITY_OF_OMISSION / partitionCount
            //
            // and finally
            //
            // partitionLimit = inverseSurvivalProbability of (PROBABILITY_OF_OMISSION / partitionCount)
            // for P_1Overflow distribution
            //
            //endregion

            int partitionLimit = partitionLimitCache.computeIfAbsent(options.getLimit(), topK -> {
                var bd = BinomialDistribution.of(topK, pSuccess);
                var estimatedPartitionLimit = bd.inverseSurvivalProbability(acceptableOmissionProbability1Partition);
                logger.info(String.format("Calculated partitionLimit = %d for topK = %d", estimatedPartitionLimit, topK));
                return estimatedPartitionLimit;
            });

            logger.finest("Using partitionLimit = %d for %s", partitionLimit, options);
            SearchOptionsBuilder effectiveOptionsBuilder = options.toBuilder()
                    .hint(Hints.PARTITION_LIMIT, partitionLimit);
            Integer maybeEfSearch = Hints.EF_SEARCH.get(options);
            if (maybeEfSearch != null && maybeEfSearch < partitionLimit) {
                if (maybeEfSearch * partitionCount < options.getLimit()) {
                    // early validation of efSearch so we do not fix it if it unfixable
                    return CompletableFuture.failedFuture(
                            new IllegalArgumentException("efSearch is not sufficient to generate full requested result"));
                }
                // fix partitionLimit to accommodate requested efSearch
                // and warn the user about possible quality degradation
                logger.info(String.format("Configured efSearch (%d) it lower than default partitionLimit (%d). "
                        + "This can worsen quality of the results. "
                        + "To disable this warning set partitionLimit explicitly.", maybeEfSearch, partitionLimit));
                effectiveOptionsBuilder.hint(Hints.PARTITION_LIMIT, maybeEfSearch);
            } else if (maybeEfSearch == null) {
                // Until we have a better heuristics for efSearch, keep the behavior as before.
                // Default efSearch is equal to partitionLimit, so decreasing partitionLimit would worsen quality.
                // We want to keep previous quality and also this is better especially for low-topK searches
                // which would get even smaller partition limit.
                effectiveOptionsBuilder.hint(Hints.EF_SEARCH, options.getLimit());
            }
            effectiveOptions = effectiveOptionsBuilder.build();
        } else {
            // User explicitly provided partitionLimit, so use it,
            // or we have only 1 partition or topK = 1, in which case PARTITION_LIMIT hint would be equal to topK anyway.
            effectiveOptions = options;
        }

        return delegate.search(collectionName, vectors, effectiveOptions, endpoint);
    }

    @Override
    public Searcher getBaseSearcher() {
        return delegate.getBaseSearcher();
    }

    @Override
    public long heapBytesUsed() {
        return FIXED_HEAP_BYTES_USED
                // count only keys and values, not the underlying hashmap structures
                + partitionLimitCache.size() * 2L * REFERENCE_COST_IN_BYTES
                + delegate.heapBytesUsed();
    }
}
