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

package com.hazelcast.sql.impl.exec.root;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.sql.impl.row.Row;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Blocking array-based result consumer which delivers the results to API caller.
 * <p>
 * Since results are then exposed to end users, it also implements {@link QueryResultProducer}.
 */
public class BlockingRootResultConsumer implements RootResultConsumer {
    /** Maximum size. */
    private final int pageSize;

    /** Mutex for concurrency support. */
    private final Object mux = new Object();

    /** Currently available rows. */
    private final ArrayDeque<Row> rows = new ArrayDeque<>();

    /** Reference to the iterator. */
    private final InternalIterator iterator = new InternalIterator();

    /** Whether we are done. */
    private boolean done;

    /** Error which occurred during query execution. */
    private HazelcastSqlException doneError;

    /** Whether root update already scheduled. */
    private boolean scheduled;

    /** Query root. */
    private QueryFragmentContext context;

    public BlockingRootResultConsumer(int pageSize) {
        this.pageSize = pageSize;
    }

    @Override
    public void setup(QueryFragmentContext context) {
        synchronized (mux) {
            this.context = context;

            mux.notifyAll();
        }
    }

    @Override
    public boolean consume(Iterable<Row> source) {
        synchronized (mux) {
            int remaining = pageSize - rows.size();

            if (remaining == 0) {
                return false;
            }

            boolean added = false;
            boolean consumed = true;

            for (Row row : source) {
                rows.add(row);

                added = true;

                if (--remaining == 0) {
                    consumed = false;

                    break;
                }
            }

            scheduled = false;

            if (added) {
                mux.notifyAll();
            }

            return consumed;
        }
    }

    @Override
    public void onDone() {
        onDone(null);
    }

    @Override
    public void onError(HazelcastSqlException error) {
        onDone(error);
    }

    private void onDone(HazelcastSqlException error) {
        synchronized (mux) {
            if (!done) {
                done = true;
                doneError = error;

                mux.notifyAll();
            }
        }
    }

    @Override
    public Iterator<Row> iterator() {
        return iterator;
    }

    /**
     * Iterator over results.
     */
    private class InternalIterator implements Iterator<Row> {

        private Row currentRow;

        @Override
        public boolean hasNext() {
            advanceIfNeeded();

            return currentRow != null;
        }

        @Override
        public Row next() {
            advanceIfNeeded();

            Row res = currentRow;

            currentRow = null;

            if (res == null) {
                throw new NoSuchElementException();
            }

            return res;
        }

        private void advanceIfNeeded() {
            if (currentRow != null) {
                return;
            }

            currentRow = advance0();
        }

        private Row advance0() {
            synchronized (mux) {
                while (true) {
                    Row row = rows.poll();

                    if (row != null) {
                        return row;
                    }

                    if (done) {
                        if (doneError != null) {
                            throw doneError;
                        }

                        return null;
                    } else {
                        // Schedule root advance if needed.
                        if (context != null && !scheduled) {
                            context.schedule();

                            scheduled = true;
                        }

                        try {
                            mux.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();

                            throw new RuntimeException("Thread was interrupted while waiting for more results.", e);
                        }
                    }
                }
            }
        }
    }
}
