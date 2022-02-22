/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.futures;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.util.iterator.RestartingMemberIterator;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ChainingFutureTest extends HazelcastTestSupport {

    private Executor executor = CALLER_RUNS;
    private ClusterService clusterService = mock(ClusterService.class);
    private RestartingMemberIterator repairingIterator;

    @Before
    public void setup() {
        Set<Member> members = new HashSet<>();
        members.add(mock(Member.class));
        when(clusterService.getMembers()).thenReturn(members);
        repairingIterator = new RestartingMemberIterator(clusterService, 100);
    }

    @Test
    public void testCompletesOnlyWhenTheLastFutureCompletes() {
        InternalCompletableFuture<Object> future1 = newFuture();
        InternalCompletableFuture<Object> future2 = newFuture();
        InternalCompletableFuture<Object> future3 = newFuture();

        CountingIterator<InternalCompletableFuture<Object>> iterator = toIterator(future1, future2, future3);

        ChainingFuture.ExceptionHandler handler = repairingIterator;
        ChainingFuture<Object> future = new ChainingFuture<>(iterator, handler);

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
        InternalCompletableFuture<Object> future1 = newFuture();
        InternalCompletableFuture<Object> future2 = newFuture();
        InternalCompletableFuture<Object> future3 = newFuture();

        CountingIterator<InternalCompletableFuture<Object>> iterator = toIterator(future1, future2, future3);

        ChainingFuture.ExceptionHandler handler = repairingIterator;
        ChainingFuture<Object> future = new ChainingFuture<>(iterator, handler);

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
    public void testNonTopologyRelatedExceptionArePropagated() {
        InternalCompletableFuture<Object> future1 = newFuture();
        InternalCompletableFuture<Object> future2 = newFuture();
        InternalCompletableFuture<Object> future3 = newFuture();

        CountingIterator<InternalCompletableFuture<Object>> iterator = toIterator(future1, future2, future3);

        ChainingFuture.ExceptionHandler handler = repairingIterator;
        ChainingFuture<Object> future = new ChainingFuture<>(iterator, handler);

        assertEquals(1, iterator.getHasNextCounter());
        assertEquals(1, iterator.getNextCounter());
        assertFalse(future.isDone());

        future1.completeExceptionally(new OperationTimeoutException());

        assertTrue(future.isDone());
        future.joinInternal();
    }

    @Test(expected = HazelcastException.class)
    public void testIteratingExceptionArePropagated() {
        InternalCompletableFuture<Object> future1 = newFuture();
        InternalCompletableFuture<Object> future2 = newFuture();
        InternalCompletableFuture<Object> future3 = newFuture();

        CountingIterator<InternalCompletableFuture<Object>> iterator = toIterator(future1, future2, future3);

        ChainingFuture.ExceptionHandler handler = repairingIterator;
        ChainingFuture<Object> future = new ChainingFuture<>(iterator, handler);

        assertEquals(1, iterator.getHasNextCounter());
        assertEquals(1, iterator.getNextCounter());
        assertFalse(future.isDone());

        iterator.exceptionToThrow = new HazelcastException("iterating exception");
        future1.complete("foo");

        assertTrue(future.isDone());
        future.joinInternal();
    }

    @Test
    public void testEmptyIterator() {
        CountingIterator<InternalCompletableFuture<Object>> iterator = toIterator();
        ChainingFuture.ExceptionHandler handler = repairingIterator;
        ChainingFuture<Object> future = new ChainingFuture<>(iterator, handler);

        assertTrue(future.isDone());
    }

    private CountingIterator<InternalCompletableFuture<Object>> toIterator(InternalCompletableFuture<Object>... futures) {
        List<InternalCompletableFuture<Object>> myCompletableFutures = asList(futures);
        return new CountingIterator<>(myCompletableFutures.iterator());
    }

    private InternalCompletableFuture<Object> newFuture() {
        return InternalCompletableFuture.withExecutor(executor);
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

        int getNextCounter() {
            return nextCounter.get();
        }

        int getHasNextCounter() {
            return hasNextCounter.get();
        }
    }


}
