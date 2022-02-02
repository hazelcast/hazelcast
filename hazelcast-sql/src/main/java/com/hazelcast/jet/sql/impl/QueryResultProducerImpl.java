/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl;

import com.hazelcast.internal.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.internal.util.concurrent.IdleStrategy;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.ResultLimitReachedException;
import com.hazelcast.sql.impl.row.JetSqlRow;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.DONE;
import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.TIMEOUT;
import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.YES;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class QueryResultProducerImpl implements QueryResultProducer {

    static final int QUEUE_CAPACITY = 4096;

    private static final Exception NORMAL_COMPLETION = new NormalCompletionException();

    private final boolean blockForNextItem;

    private final OneToOneConcurrentArrayQueue<JetSqlRow> rows = new OneToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
    private final AtomicReference<Exception> done = new AtomicReference<>();

    private InternalIterator iterator;
    private long limit = Long.MAX_VALUE;
    private long offset;

    /**
     * If {@code blockForNextItem} is true, the iterator's {@code
     * hasNext(timeout)} method will block until a next item is available or
     * iteration is done; the timeout will be ignored. This is suitable for
     * batch jobs where low streaming latency isn't required, but rather we
     * want to return full pages of results to the client.
     */
    public QueryResultProducerImpl(boolean blockForNextItem) {
        this.blockForNextItem = blockForNextItem;
    }

    public void init(long limit, long offset) {
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    public ResultIterator<JetSqlRow> iterator() {
        if (iterator != null) {
            throw new IllegalStateException("Iterator can be requested only once");
        }
        iterator = new InternalIterator();
        return iterator;
    }

    @Override
    public void onError(QueryException error) {
        assert error != null;
        done.compareAndSet(null, error);
    }

    public void done() {
        done.compareAndSet(null, NORMAL_COMPLETION);
    }

    public void consume(Inbox inbox) {
        ensureNotDone();
        if (limit <= 0) {
            done.compareAndSet(null, new ResultLimitReachedException());
            ensureNotDone();
        }

        while (offset > 0 && inbox.poll() != null) {
            offset--;
        }

        for (JetSqlRow row; (row = (JetSqlRow) inbox.peek()) != null && rows.offer(row); ) {
            inbox.remove();
            if (limit != Long.MAX_VALUE) {
                limit -= 1;
                if (limit < 1) {
                    done.compareAndSet(null, new ResultLimitReachedException());
                    ensureNotDone();
                }
            }
        }
    }

    public void ensureNotDone() {
        Exception exception = done.get();
        if (exception != null) {
            throw sneakyThrow(exception);
        }
    }

    private class InternalIterator implements ResultIterator<JetSqlRow> {

        private final IdleStrategy idler =
                new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(50), MILLISECONDS.toNanos(1));

        private JetSqlRow nextRow;

        @Override
        public HasNextResult hasNext(long timeout, TimeUnit timeUnit) {
            if (blockForNextItem) {
                return hasNext() ? YES : DONE;
            }
            return nextRow != null || (nextRow = rows.poll()) != null ? YES
                    : isDone() ? DONE
                    : timeout == 0 ? TIMEOUT
                    : hasNextWait(System.nanoTime() + timeUnit.toNanos(timeout));
        }

        @Override
        public boolean hasNext() {
            return hasNextWait(Long.MAX_VALUE) == YES;
        }

        @Override
        public JetSqlRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                return nextRow;
            } finally {
                nextRow = rows.poll();
            }
        }

        private HasNextResult hasNextWait(long endTimeNanos) {
            long idleCount = 0;
            do {
                if (nextRow != null || (nextRow = rows.poll()) != null) {
                    return YES;
                }
                if (isDone()) {
                    return DONE;
                }
                idler.idle(++idleCount);
                if (Thread.currentThread().isInterrupted()) {
                    // We want to allow interruption of a thread that is blocked in `hasNext()`. However, the
                    // `hasNext()` method doesn't declare the InterruptedException. Therefore, we throw
                    // the interrupted exception wrapped in a RuntimeException. The interrupted status
                    // if the current thread remains set.
                    throw new RuntimeException(new InterruptedException("thread interrupted"));
                }
            } while (System.nanoTime() < endTimeNanos);
            return TIMEOUT;
        }

        /**
         * Returns:<ul>
         * <li>true, if done and rows are exhausted
         * <li>false, if not done or rows are not exhausted
         * <li>throws exception, if done with error
         * </ul>
         */
        private boolean isDone() {
            Exception exception = done.get();
            if (exception != null) {
                if (exception instanceof NormalCompletionException || exception instanceof ResultLimitReachedException) {
                    // finish the rows first
                    return rows.isEmpty();
                }
                throw sneakyThrow(exception);
            }
            return false;
        }
    }

    private static final class NormalCompletionException extends Exception {
        NormalCompletionException() {
            // Use writableStackTrace = false, the exception is not created at a place where it's thrown,
            // it's better if it has no stack trace then.
            super("Done normally", null, false, false);
        }
    }
}
