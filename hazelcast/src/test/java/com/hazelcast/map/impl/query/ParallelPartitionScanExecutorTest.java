/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.executor.NamedThreadPoolExecutor;
import com.hazelcast.util.executor.PoolExecutorThreadFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ParallelPartitionScanExecutorTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private ParallelPartitionScanExecutor executor(PartitionScanRunner runner) {
        PoolExecutorThreadFactory threadFactory = new PoolExecutorThreadFactory(UUID.randomUUID().toString(),
                currentThread().getContextClassLoader());
        NamedThreadPoolExecutor pool = new NamedThreadPoolExecutor(UUID.randomUUID().toString(), 1, 1, 100, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(100), threadFactory);
        return new ParallelPartitionScanExecutor(runner, pool, 60000);
    }

    @Test
    public void execute_success() {
        PartitionScanRunner runner = mock(PartitionScanRunner.class);
        ParallelPartitionScanExecutor executor = executor(runner);
        Predicate predicate = Predicates.equal("attribute", 1);
        QueryResult queryResult = new QueryResult(IterationType.ENTRY, null, null, Long.MAX_VALUE, false);

        executor.execute("Map", predicate, asList(1, 2, 3), queryResult);
        List<QueryResultRow> result = queryResult.getRows();
        assertEquals(0, result.size());
    }

    @Test
    public void execute_fail() {
        PartitionScanRunner runner = mock(PartitionScanRunner.class);
        ParallelPartitionScanExecutor executor = executor(runner);
        Predicate predicate = Predicates.equal("attribute", 1);
        QueryResult queryResult = new QueryResult(IterationType.ENTRY, null, null, Long.MAX_VALUE, false);

        doThrow(new QueryException()).when(runner).run(anyString(), eq(predicate), anyInt(), isA(QueryResult.class));

        expected.expect(QueryException.class);
        executor.execute("Map", predicate, asList(1, 2, 3), queryResult);
    }

    @Test
    public void execute_fail_retryable() {
        PartitionScanRunner runner = mock(PartitionScanRunner.class);
        ParallelPartitionScanExecutor executor = executor(runner);
        Predicate predicate = Predicates.equal("attribute", 1);
        QueryResult queryResult = new QueryResult(IterationType.ENTRY, null, null, Long.MAX_VALUE, false);

        doThrow(new RetryableHazelcastException()).when(runner).run(anyString(), eq(predicate), anyInt(), isA(QueryResult.class));

        expected.expect(RetryableHazelcastException.class);
        executor.execute("Map", predicate, asList(1, 2, 3), queryResult);
    }
}
