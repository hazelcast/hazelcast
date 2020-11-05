/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.DONE;
import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.TIMEOUT;
import static com.hazelcast.sql.impl.ResultIterator.HasNextResult.YES;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class JetQueryResultProducer implements QueryResultProducer {

    static final int QUEUE_CAPACITY = 4096;

    private static final Exception NORMAL_COMPLETION = new NormalCompletionException();

    private final OneToOneConcurrentArrayQueue<Row> queue = new OneToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
    private final AtomicReference<Exception> done = new AtomicReference<>();
    private InternalIterator iterator;

    @Override
    public ResultIterator<Row> iterator() {
        if (iterator != null) {
            throw new IllegalStateException("The iterator can be requested only once");
        }
        iterator = new InternalIterator();
        return iterator;
    }

    @Override
    public void onError(QueryException error) {
        done.compareAndSet(null, error);
    }

    public void done() {
        done.compareAndSet(null, NORMAL_COMPLETION);
    }

    public void consume(Inbox inbox) {
        if (done.get() != null) {
            throw new RuntimeException(done.get());
        }
        for (Object[] r; (r = (Object[]) inbox.peek()) != null && queue.offer(new HeapRow(r)); ) {
            inbox.remove();
        }
    }

    private class InternalIterator implements ResultIterator<Row> {

        private final IdleStrategy idler =
                new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(50), MILLISECONDS.toNanos(1));

        private Row nextRow;

        @Override
        public HasNextResult hasNext(long timeout, TimeUnit timeUnit) {
            return nextRow != null || (nextRow = queue.poll()) != null ? YES
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
                nextRow = queue.poll();
            }
        }

        private HasNextResult hasNextWait(long endTimeNanos) {
            long idleCount = 0;
            do {
                if (nextRow != null || (nextRow = queue.poll()) != null) {
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
         *     <li>true, if done
         *     <li>false, if not done
         *     <li>throws exception, if done with error
         * </ul>
         */
        private boolean isDone() {
            Exception doneExc = done.get();
            if (doneExc != null) {
                if (doneExc instanceof NormalCompletionException) {
                    return true;
                }
                throw new RuntimeException("The Jet SQL job failed: " + doneExc.getMessage(), doneExc);
            }
            return false;
        }
    }

    private static final class NormalCompletionException extends Exception {
        NormalCompletionException() {
            super("done normally");
        }
    }
}
