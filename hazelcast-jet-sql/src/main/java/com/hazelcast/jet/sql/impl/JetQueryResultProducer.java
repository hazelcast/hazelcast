/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.internal.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.internal.util.concurrent.IdleStrategy;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.Row;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.DONE;
import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.TIMEOUT;
import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.YES;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class JetQueryResultProducer implements QueryResultProducer {

    static final int QUEUE_CAPACITY = 4096;

    private static final Exception NORMAL_COMPLETION = new NormalCompletionException();

    private final OneToOneConcurrentArrayQueue<Row> rows = new OneToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
    private final AtomicReference<Exception> done = new AtomicReference<>();

    private InternalIterator iterator;

    @Override
    public ResultIterator<Row> iterator() {
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
        for (Object[] row; (row = (Object[]) inbox.peek()) != null && rows.offer(new HeapRow(row)); ) {
            inbox.remove();
        }
    }

    public void ensureNotDone() {
        Exception exception = done.get();
        if (exception != null) {
            throw sneakyThrow(exception);
        }
    }

    private class InternalIterator implements ResultIterator<Row> {

        private final IdleStrategy idler =
                new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(50), MILLISECONDS.toNanos(1));

        private Row nextRow;

        @Override
        public HasNextResult hasNext(long timeout, TimeUnit timeUnit) {
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
        public Row next() {
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
                if (exception instanceof NormalCompletionException) {
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
