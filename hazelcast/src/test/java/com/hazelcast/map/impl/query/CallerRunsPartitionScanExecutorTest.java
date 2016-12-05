package com.hazelcast.map.impl.query;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static java.util.Arrays.asList;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CallerRunsPartitionScanExecutorTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void execute_success() throws Exception {
        PartitionScanRunner runner = mock(PartitionScanRunner.class);
        CallerRunsPartitionScanExecutor executor = new CallerRunsPartitionScanExecutor(runner);
        Predicate predicate = Predicates.equal("attribute", 1);

        executor.execute("Map", predicate, asList(1, 2, 3));
    }

    @Test
    public void execute_fail() throws Exception {
        PartitionScanRunner runner = mock(PartitionScanRunner.class);
        CallerRunsPartitionScanExecutor executor = new CallerRunsPartitionScanExecutor(runner);
        Predicate predicate = Predicates.equal("attribute", 1);

        when(runner.run(anyString(), eq(predicate), anyInt())).thenThrow(new QueryException());

        expected.expect(QueryException.class);
        executor.execute("Map", predicate, asList(1, 2, 3));
    }

    @Test
    public void execute_fail_retryable() throws Exception {
        PartitionScanRunner runner = mock(PartitionScanRunner.class);
        CallerRunsPartitionScanExecutor executor = new CallerRunsPartitionScanExecutor(runner);
        Predicate predicate = Predicates.equal("attribute", 1);

        when(runner.run(anyString(), eq(predicate), anyInt())).thenThrow(new RetryableHazelcastException());

        expected.expect(RetryableHazelcastException.class);
        executor.execute("Map", predicate, asList(1, 2, 3));
    }


}