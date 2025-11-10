/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.internal.util.IterationType;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CallerRunsPartitionScanExecutorTest {

    @Test
    public void execute_success() {
        PartitionScanRunner runner = mock(PartitionScanRunner.class);
        CallerRunsPartitionScanExecutor executor = new CallerRunsPartitionScanExecutor(runner);
        Predicate predicate = Predicates.equal("attribute", 1);
        QueryResult queryResult = new QueryResult(IterationType.ENTRY, null, null, Long.MAX_VALUE, false);

        executor.execute("Map", predicate, asList(1, 2, 3), queryResult);
        Collection<QueryResultRow> result = queryResult.getRows();
        assertEquals(0, result.size());
    }

    @Test
    public void execute_fail() {
        PartitionScanRunner runner = mock(PartitionScanRunner.class);
        CallerRunsPartitionScanExecutor executor = new CallerRunsPartitionScanExecutor(runner);
        Predicate predicate = Predicates.equal("attribute", 1);
        QueryResult queryResult = new QueryResult(IterationType.ENTRY, null, null, Long.MAX_VALUE, false);

        doThrow(new QueryException()).when(runner).run(anyString(), eq(predicate), anyInt(), eq(queryResult));

        List<Integer> list = asList(1, 2, 3);
        assertThatThrownBy(() -> executor.execute("Map", predicate, list, queryResult))
                .isInstanceOf(QueryException.class);
    }

    @Test
    public void execute_fail_retryable() {
        PartitionScanRunner runner = mock(PartitionScanRunner.class);
        CallerRunsPartitionScanExecutor executor = new CallerRunsPartitionScanExecutor(runner);
        Predicate predicate = Predicates.equal("attribute", 1);
        QueryResult queryResult = new QueryResult(IterationType.ENTRY, null, null, Long.MAX_VALUE, false);

        doThrow(new RetryableHazelcastException()).when(runner).run(anyString(), eq(predicate), anyInt(), eq(queryResult));

        List<Integer> list = asList(1, 2, 3);
        assertThatThrownBy(() -> executor.execute("Map", predicate, list, queryResult))
                .isInstanceOf(RetryableHazelcastException.class);
    }
}
