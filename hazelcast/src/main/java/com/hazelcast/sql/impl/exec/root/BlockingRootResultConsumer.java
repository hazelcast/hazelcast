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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Blocking array-based result consumer which delivers the results to API caller.
 */
public class BlockingRootResultConsumer implements RootResultConsumer {
    /** Mutex for concurrency support. */
    private final Object mux = new Object();

    /** Iterator over produced rows. */
    private final InternalIterator iterator = new InternalIterator();

    /** Query context to schedule root execution when the next batch is needed. */
    private volatile QueryFragmentContext context;

    /** The batch that is currently being consumed. */
    private List<Row> currentBatch;

    /** When "true" no more batches are expected. */
    private boolean done;

    /** Error which occurred during query execution. */
    private QueryException doneError;

    @Override
    public void setup(QueryFragmentContext context) {
        this.context = context;
    }

    @Override
    public boolean consume(List<Row> batch, boolean last) {
        synchronized (mux) {
            assert !done;

            if (currentBatch == null) {
                if (!batch.isEmpty()) {
                    currentBatch = batch;
                }

                if (last) {
                    done = true;
                }

                mux.notifyAll();

                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public void onError(QueryException error) {
        synchronized (mux) {
            if (!done) {
                done = true;
                doneError = error;

                mux.notifyAll();
            }
        }
    }

    /**
     * Poll the next batch from the upstream, waiting if needed.
     *
     * @return The batch or {@code null} if end of stream is reached.
     */
    private List<Row> awaitNextBatch() {
        synchronized (mux) {
            while (true) {
                // Consume the batch if it is available.
                if (currentBatch != null) {
                    List<Row> res = currentBatch;

                    currentBatch = null;

                    return res;
                }

                // Handle end of the stream.
                if (done) {
                    if (doneError != null) {
                        throw doneError;
                    }

                    return null;
                }

                // Otherwise wait.
                try {
                    mux.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw QueryException.error("Thread was interrupted while waiting for more results.", e);
                }
            }
        }
    }

    /**
     * Request the next batch from the executor.
     */
    private void requestNextBatch() {
        synchronized (mux) {
            if (done) {
                return;
            }
        }

        // We may reach this place only if some rows are already produced, and this is possible only after the setup,
        // so the context should be initialized.
        assert context != null;

        context.schedule();
    }

    @Override
    public Iterator<Row> iterator() {
        return iterator;
    }

    /**
     * Iterator over results.
     */
    private class InternalIterator implements Iterator<Row> {

        private List<Row> batch;
        private int position;

        @Override
        public boolean hasNext() {
            if (batch == null) {
                batch = awaitNextBatch();

                if (batch == null) {
                    assert done;

                    return false;
                }
            }

            return true;
        }

        @Override
        public Row next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            assert batch != null;

            Row res = batch.get(position++);

            if (position == batch.size()) {
                batch = null;
                position = 0;

                requestNextBatch();
            }

            return res;
        }
    }
}
