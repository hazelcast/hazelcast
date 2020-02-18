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

package com.hazelcast.sql.impl;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.exec.RootExec;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Blocking array-based result consumer.
 */
public class QueryResultConsumerImpl implements QueryResultConsumer {
    /** Default batch size. */
    private static final int DFLT_BATCH_SIZE = 1024;

    /** Maximum size. */
    private final int maxSize;

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
    private boolean rootScheduled;

    /** Query root. */
    private RootExec root;

    public QueryResultConsumerImpl() {
        this(DFLT_BATCH_SIZE);
    }

    public QueryResultConsumerImpl(int maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public void setup(RootExec root) {
        synchronized (mux) {
            this.root = root;

            mux.notifyAll();
        }
    }

    @Override
    public int consume(RowBatch batch, int startPos) {
        synchronized (mux) {
            int available = batch.getRowCount() - startPos;
            int remaining = maxSize - rows.size();

            int toConsume = Math.min(available, remaining);

            for (int i = startPos; i < toConsume; i++) {
                rows.add(batch.getRow(i));
            }

            rootScheduled = false;

            mux.notifyAll();

            return toConsume;
        }
    }

    @Override
    public boolean consume(Iterable<Row> source) {
        synchronized (mux) {
            int remaining = maxSize - rows.size();

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

            rootScheduled = false;

            if (added) {
                mux.notifyAll();
            }

            return consumed;
        }
    }

    @Override
    public void done() {
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
    public Iterator<SqlRow> iterator() {
        return iterator;
    }

    /**
     * Iterator over results.
     */
    private class InternalIterator implements Iterator<SqlRow> {
        /** Current row. */
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
                        if (root != null && !rootScheduled) {
                            root.reschedule();

                            rootScheduled = true;
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
