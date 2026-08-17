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

import com.hazelcast.logging.ILogger;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.internal.impl.Hints;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static com.hazelcast.vector.internal.impl.VectorTestUtils.vec;
import static com.hazelcast.vector.internal.impl.proxy.VectorCollectionProxyTest.TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class PartitionLimitEstimatingSearcherTest {
    @Mock
    Searcher delegate;
    @Mock
    ILogger logger;

    final String name = "vectorCollection";
    final VectorValues vectors = vec(1f);

    @ParameterizedTest
    @ValueSource(ints = {2, 10, 271})
    void shouldNotPassPartitionLimitForTopK1(int partitionCount) {
        PartitionLimitEstimatingSearcher searcher = new PartitionLimitEstimatingSearcher(logger, partitionCount, delegate);

        final SearchOptions options = SearchOptions.builder().limit(1).build();

        searcher.search(name, vectors, options);

        verifyLimitAndPartitionLimit(1, null, null);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 100})
    void shouldNotPassPartitionLimitFor1Partition(int limit) {
        PartitionLimitEstimatingSearcher searcher = new PartitionLimitEstimatingSearcher(logger, 1, delegate);

        final SearchOptions options = SearchOptions.builder().limit(limit).build();

        searcher.search(name, vectors, options);

        verifyLimitAndPartitionLimit(limit, null, null);
    }

    @Test
    void shouldCalculatePartitionLimit() {
        PartitionLimitEstimatingSearcher searcher = new PartitionLimitEstimatingSearcher(logger, 16, delegate);

        final SearchOptions options = SearchOptions.builder().limit(10).build();

        searcher.search(name, vectors, options);

        verifyLimitAndPartitionLimit(10, 6);
    }

    @Test
    void shouldCalculatePartitionLimitWithDefaultPartitionCount() {
        PartitionLimitEstimatingSearcher searcher = new PartitionLimitEstimatingSearcher(logger, 271, delegate);

        final SearchOptions options = SearchOptions.builder().limit(100).build();

        searcher.search(name, vectors, options);

        // by pure chance partitionLimit is the same as for topK=10 with 16 partitions
        verifyLimitAndPartitionLimit(100, 6);
    }

    @Test
    void shouldCalculatePartitionLimitWithBigPartitionCount() {
        PartitionLimitEstimatingSearcher searcher = new PartitionLimitEstimatingSearcher(logger, 5009, delegate);

        final SearchOptions options = SearchOptions.builder().limit(100).build();

        searcher.search(name, vectors, options);

        verifyLimitAndPartitionLimit(100, 3);
    }

    @Test
    void shouldCalculatePartitionLimitWithBigPartitionCountAndLimit() {
        PartitionLimitEstimatingSearcher searcher = new PartitionLimitEstimatingSearcher(logger, 5009, delegate);

        // check for possible overflow/limits in the calculation
        final SearchOptions options = SearchOptions.builder().limit(100000).build();

        searcher.search(name, vectors, options);

        verifyLimitAndPartitionLimit(100000, 49);
    }

    @Test
    void shouldNotOverwriteExplicitPartitionLimit() {
        PartitionLimitEstimatingSearcher searcher = new PartitionLimitEstimatingSearcher(logger, 16, delegate);

        final SearchOptions options = SearchOptions.builder()
                .limit(10)
                .hint(Hints.PARTITION_LIMIT, 2)
                .build();

        searcher.search(name, vectors, options);

        // with explicit partitionLimit efSearch should not be set
        verifyLimitAndPartitionLimit(10, 2, null);
    }

    @Test
    void shouldNotOverwriteExplicitEfSearch() {
        PartitionLimitEstimatingSearcher searcher = new PartitionLimitEstimatingSearcher(logger, 16, delegate);

        final SearchOptions options = SearchOptions.builder()
                .limit(10)
                .hint(Hints.EF_SEARCH, 100)
                .build();

        searcher.search(name, vectors, options);

        verifyLimitAndPartitionLimit(10, 6, 100);
    }

    @Test
    void shouldDecreasePartitionLimitWhenExplicitEfSearchIsLowerThanDefaultPartitionLimit() {
        PartitionLimitEstimatingSearcher searcher = new PartitionLimitEstimatingSearcher(logger, 16, delegate);

        final SearchOptions options = SearchOptions.builder()
                .limit(10)
                .hint(Hints.EF_SEARCH, 3)
                .build();

        searcher.search(name, vectors, options);

        // normally default partitionLimit would be 6
        verifyLimitAndPartitionLimit(10, 3, 3);
    }

    @Test
    void shouldFailWhenExplicitEfSearchIsTooLow() {
        PartitionLimitEstimatingSearcher searcher = new PartitionLimitEstimatingSearcher(logger, 16, delegate);

        final SearchOptions options = SearchOptions.builder()
                .limit(20)
                .hint(Hints.EF_SEARCH, 1)
                .build();

        assertThat(searcher.search(name, vectors, options))
                .failsWithin(TIMEOUT)
                .withThrowableThat().havingRootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .withMessageStartingWith("efSearch is not sufficient to generate full requested result");
    }

    private void verifyLimitAndPartitionLimit(int limit, Integer partitionLimit) {
        // backward compatible behavior: efSearch is equal to limit
        verifyLimitAndPartitionLimit(limit, partitionLimit, limit);
    }

    private void verifyLimitAndPartitionLimit(int limit, Integer partitionLimit, Integer efSearch) {
        var optionsCaptor = ArgumentCaptor.forClass(SearchOptions.class);
        verify(delegate).search(eq(name), eq(vectors), optionsCaptor.capture(), eq(null));
        assertThat(optionsCaptor.getValue().getLimit()).isEqualTo(limit);
        assertThat(Hints.PARTITION_LIMIT.get(optionsCaptor.getValue())).isEqualTo(partitionLimit);
        assertThat(Hints.EF_SEARCH.get(optionsCaptor.getValue())).isEqualTo(efSearch);
    }
}
