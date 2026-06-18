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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.vector.SearchResults;
import org.junit.jupiter.api.Test;

import static com.hazelcast.vector.impl.VectorTestUtils.srData;
import static com.hazelcast.vector.impl.VectorTestUtils.srs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryResultTest {

    @Test
    void shouldRejectTooManyResults() {
        var qr = new QueryResult(3, 2, 10);
        qr.addResult(0, srs(srData("a", 1)));
        qr.addResult(1, srs(srData("a", 1)));
        assertThatThrownBy(() -> qr.addResult(2, srs(srData("a", 1)))).isInstanceOf(IllegalStateException.class);
        assertThat(qr.complete().results()).toIterable().hasSize(2);
    }

    @Test
    void shouldRejectDuplicateResult_whenAssertionsEnabled() {
        var qr = new QueryResult(2, 2, 10);
        qr.addResult(0, srs(srData("a", 1)));
        assertThatThrownBy(() -> qr.addResult(0, srs(srData("a", 1)))).isInstanceOf(AssertionError.class);
    }

    @Test
    void shouldReturnNothing_whenNoResultsAdded() {
        var qr = new QueryResult(1, 1, 10);

        var complete = qr.complete();
        assertThat(complete.size()).isZero();
        assertThat(complete.results()).toIterable().isEmpty();
    }

    @Test
    void shouldReturnNothing_whenEmptyResultAdded() {
        var qr = new QueryResult(1, 1, 10);
        qr.addResult(0, srs());

        var complete = qr.complete();
        assertThat(complete.size()).isZero();
        assertThat(complete.results()).toIterable().isEmpty();
    }

    @Test
    void shouldReturnNothing_whenEmptyResultsAdded() {
        var qr = new QueryResult(2, 2, 10);
        qr.addResult(0, srs());
        qr.addResult(1, srs());

        var complete = qr.complete();
        assertThat(complete.size()).isZero();
        assertThat(complete.results()).toIterable().isEmpty();
    }

    @Test
    void shouldReturnData_whenSinglePartition() {
        var qr = new QueryResult(1, 1, 10);
        qr.addResult(0, srs(srData("a", 1), srData("b", 0.9f)));

        SearchResults<Data, Data> complete = qr.complete();
        assertThat(complete.size()).isEqualTo(2);
        assertThat(complete.results()).toIterable().containsExactly(srData("a", 1), srData("b", 0.9f));
    }

    @Test
    void shouldReturnData_whenSinglePartitionWithResultsAndOtherEmpty() {
        var qr = new QueryResult(3, 3, 10);
        qr.addResult(0, srs(srData("a", 1), srData("b", 0.9f)));
        qr.addResult(1, srs());
        qr.addResult(2, srs());

        assertThat(qr.complete().results()).toIterable().containsExactly(srData("a", 1), srData("b", 0.9f));
    }

    @Test
    void shouldMergeDataFromManyPartitions() {
        var qr = new QueryResult(2, 2, 10);
        qr.addResult(0, srs(srData("a", 1), srData("b", 0.8f)));
        qr.addResult(1, srs(srData("c", 0.99f), srData("d", 0.9f)));

        var complete = qr.complete();
        assertThat(complete.size()).isEqualTo(4);
        assertThat(complete.results()).toIterable().containsExactly(srData("a", 1), srData("c", 0.99f),
                srData("d", 0.9f), srData("b", 0.8f));
    }

    @Test
    void shouldMergeAndLimitDataFromManyPartitions() {
        var qr = new QueryResult(2, 2, 2);
        qr.addResult(0, srs(srData("a", 1), srData("b", 0.8f)));
        qr.addResult(1, srs(srData("c", 0.99f), srData("d", 0.9f)));

        var complete = qr.complete();
        assertThat(complete.size()).isEqualTo(2);
        assertThat(complete.results()).toIterable().containsExactly(srData("a", 1), srData("c", 0.99f));
    }

    @Test
    void shouldMergeAndLimitDataFromManyPartitions_whenSinglePartitionHasAllBestResults() {
        var qr = new QueryResult(2, 2, 2);
        qr.addResult(0, srs(srData("a", 0.9f), srData("b", 0.8f)));
        qr.addResult(1, srs(srData("c", 1f), srData("d", 0.99f)));

        var complete = qr.complete();
        assertThat(complete.size()).isEqualTo(2);
        assertThat(complete.results()).toIterable().containsExactly(srData("c", 1), srData("d", 0.99f));
    }
}
