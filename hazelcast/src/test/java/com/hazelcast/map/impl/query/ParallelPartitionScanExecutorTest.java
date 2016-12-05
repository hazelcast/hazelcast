package com.hazelcast.map.impl.query;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.logging.NoLogFactory;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ParallelPartitionScanExecutorTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private ParallelPartitionScanExecutor executor(PartitionScanRunner runner) {
        NoLogFactory factory = new NoLogFactory();
        HazelcastThreadGroup group = new HazelcastThreadGroup(UUID.randomUUID().toString(),
                factory.getLogger(ParallelPartitionScanExecutorTest.class.getName()), Thread.currentThread().getContextClassLoader());
        PoolExecutorThreadFactory threadFactory = new PoolExecutorThreadFactory(group, UUID.randomUUID().toString());
        NamedThreadPoolExecutor pool = new NamedThreadPoolExecutor(UUID.randomUUID().toString(), 1, 1,
                100, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(100),
                threadFactory
        );
        return new ParallelPartitionScanExecutor(runner, pool, 5);
    }

    @Test
    public void execute_success() throws Exception {
        PartitionScanRunner runner = mock(PartitionScanRunner.class);
        ParallelPartitionScanExecutor executor = executor(runner);
        Predicate predicate = Predicates.equal("attribute", 1);

        List<QueryableEntry> result = executor.execute("Map", predicate, asList(1, 2, 3));
        assertEquals(0, result.size());
    }

    @Test
    public void execute_fail() throws Exception {
        PartitionScanRunner runner = mock(PartitionScanRunner.class);
        ParallelPartitionScanExecutor executor = executor(runner);
        Predicate predicate = Predicates.equal("attribute", 1);

        when(runner.run(anyString(), eq(predicate), anyInt())).thenThrow(new QueryException());

        expected.expect(QueryException.class);
        executor.execute("Map", predicate, asList(1, 2, 3));
    }

    @Test
    public void execute_fail_retryable() throws Exception {
        PartitionScanRunner runner = mock(PartitionScanRunner.class);
        ParallelPartitionScanExecutor executor = executor(runner);
        Predicate predicate = Predicates.equal("attribute", 1);

        when(runner.run(anyString(), eq(predicate), anyInt())).thenThrow(new RetryableHazelcastException());

        expected.expect(RetryableHazelcastException.class);
        executor.execute("Map", predicate, asList(1, 2, 3));
    }

}