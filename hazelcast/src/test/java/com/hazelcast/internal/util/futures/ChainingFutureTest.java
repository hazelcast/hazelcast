package com.hazelcast.internal.util.futures;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.util.iterator.RestartingMemberIterator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ChainingFutureTest extends HazelcastTestSupport {

    private Executor executor = new LocalExecutor();
    private ILogger logger = mock(ILogger.class);
    private ClusterService clusterService = mock(ClusterService.class);
    private RestartingMemberIterator repairingIterator;

    @Before
    public void setup() {
        Set<Member> members = new HashSet<Member>();
        members.add(mock(Member.class));
        when(clusterService.getMembers()).thenReturn(members);
        repairingIterator = new RestartingMemberIterator(clusterService, 100);
    }

    @Test
    public void testCompletesOnlyWhenTheLastFutureCompletes() {
        MyCompletableFuture<Object> future1 = newFuture();
        MyCompletableFuture<Object> future2 = newFuture();
        MyCompletableFuture<Object> future3 = newFuture();

        CountingIterator<ICompletableFuture<Object>> iterator = toIterator(future1, future2, future3);

        ChainingFuture.ExceptionHandler handler = repairingIterator;
        ChainingFuture<Object> future = new ChainingFuture<Object>(iterator, executor, handler, logger);

        assertEquals(1, iterator.getHasNextCounter());
        assertEquals(1, iterator.getNextCounter());
        assertFalse(future.isDone());

        future1.complete("foo");
        assertEquals(2, iterator.getHasNextCounter());
        assertEquals(2, iterator.getNextCounter());
        assertFalse(future.isDone());

        future2.complete("foo");
        assertFalse(future.isDone());
        assertEquals(3, iterator.getHasNextCounter());
        assertEquals(3, iterator.getNextCounter());

        future3.complete("foo");
        assertTrue(future.isDone());
        assertEquals(4, iterator.getHasNextCounter());
        assertEquals(3, iterator.getNextCounter());
    }

    @Test
    public void testTopologyChangesExceptionsAreIgnored() {
        MyCompletableFuture<Object> future1 = newFuture();
        MyCompletableFuture<Object> future2 = newFuture();
        MyCompletableFuture<Object> future3 = newFuture();

        CountingIterator<ICompletableFuture<Object>> iterator = toIterator(future1, future2, future3);

        ChainingFuture.ExceptionHandler handler = repairingIterator;
        ChainingFuture<Object> future = new ChainingFuture<Object>(iterator, executor, handler, logger);

        assertEquals(1, iterator.getHasNextCounter());
        assertEquals(1, iterator.getNextCounter());
        assertFalse(future.isDone());

        future1.complete(new MemberLeftException("this should be ignored"));
        assertEquals(2, iterator.getHasNextCounter());
        assertEquals(2, iterator.getNextCounter());
        assertFalse(future.isDone());

        future2.complete(new TargetNotMemberException("this should be ignored"));
        assertEquals(3, iterator.getHasNextCounter());
        assertEquals(3, iterator.getNextCounter());
        assertFalse(future.isDone());


        future3.complete("foo");
        assertTrue(future.isDone());
        assertEquals(4, iterator.getHasNextCounter());
        assertEquals(3, iterator.getNextCounter());
    }

    @Test(expected = OperationTimeoutException.class)
    public void testNonTopologyRelatedExceptionArePropagated() throws ExecutionException, InterruptedException {
        MyCompletableFuture<Object> future1 = newFuture();
        MyCompletableFuture<Object> future2 = newFuture();
        MyCompletableFuture<Object> future3 = newFuture();

        CountingIterator<ICompletableFuture<Object>> iterator = toIterator(future1, future2, future3);

        ChainingFuture.ExceptionHandler handler = repairingIterator;
        ChainingFuture<Object> future = new ChainingFuture<Object>(iterator, executor, handler, logger);

        assertEquals(1, iterator.getHasNextCounter());
        assertEquals(1, iterator.getNextCounter());
        assertFalse(future.isDone());

        future1.complete(new OperationTimeoutException());

        assertTrue(future.isDone());
        future.get();
    }

    @Test(expected = HazelcastException.class)
    public void testIteratingExceptionArePropagated() throws ExecutionException, InterruptedException {
        MyCompletableFuture<Object> future1 = newFuture();
        MyCompletableFuture<Object> future2 = newFuture();
        MyCompletableFuture<Object> future3 = newFuture();

        CountingIterator<ICompletableFuture<Object>> iterator = toIterator(future1, future2, future3);

        ChainingFuture.ExceptionHandler handler = repairingIterator;
        ChainingFuture<Object> future = new ChainingFuture<Object>(iterator, executor, handler, logger);

        assertEquals(1, iterator.getHasNextCounter());
        assertEquals(1, iterator.getNextCounter());
        assertFalse(future.isDone());

        iterator.exceptionToThrow = new HazelcastException("iterating exception");
        future1.complete("foo");

        assertTrue(future.isDone());
        future.get();
    }

    @Test
    public void testEmptyIterator() {
        CountingIterator<ICompletableFuture<Object>> iterator = toIterator();
        ChainingFuture.ExceptionHandler handler = repairingIterator;
        ChainingFuture<Object> future = new ChainingFuture<Object>(iterator, executor, handler, logger);

        assertTrue(future.isDone());
    }

    private CountingIterator<ICompletableFuture<Object>> toIterator(ICompletableFuture<Object>...futures) {
        List<ICompletableFuture<Object>> myCompletableFutures = asList(futures);
        return new CountingIterator<ICompletableFuture<Object>>(myCompletableFutures.iterator());
    }

    private MyCompletableFuture<Object> newFuture() {
        return new MyCompletableFuture(executor, logger);
    }

    private static class CountingIterator<T> implements Iterator<T> {
        private final Iterator<T> innerIterator;
        private AtomicInteger hasNextCounter = new AtomicInteger();
        private AtomicInteger nextCounter = new AtomicInteger();
        private volatile RuntimeException exceptionToThrow;

        private CountingIterator(Iterator<T> innerIterator) {
            this.innerIterator = innerIterator;
        }

        @Override
        public boolean hasNext() {
            hasNextCounter.incrementAndGet();
            throwExceptionIfSet();
            return innerIterator.hasNext();
        }

        private void throwExceptionIfSet() {
            if (exceptionToThrow != null) {
                throw exceptionToThrow;
            }
        }

        @Override
        public T next() {
            nextCounter.incrementAndGet();
            throwExceptionIfSet();
            return innerIterator.next();
        }

        @Override
        public void remove() {
            innerIterator.remove();
        }

        public int getNextCounter() {
            return nextCounter.get();
        }

        public int getHasNextCounter() {
            return hasNextCounter.get();
        }
    }


    private static class LocalExecutor implements Executor {
        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }

    private static class MyCompletableFuture<T> extends AbstractCompletableFuture<T> {

        protected MyCompletableFuture(Executor defaultExecutor, ILogger logger) {
            super(defaultExecutor, logger);
        }

        public void complete(Object o) {
            setResult(o);
        }
    }


}
