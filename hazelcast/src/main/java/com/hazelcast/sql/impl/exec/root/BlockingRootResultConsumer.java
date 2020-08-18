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

import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.ResultIterator.HasNextImmediatelyResult;
import com.hazelcast.sql.impl.row.Row;

import java.util.List;
import java.util.NoSuchElementException;

import static com.hazelcast.sql.impl.ResultIterator.HasNextImmediatelyResult.DONE;
import static com.hazelcast.sql.impl.ResultIterator.HasNextImmediatelyResult.RETRY;
import static com.hazelcast.sql.impl.ResultIterator.HasNextImmediatelyResult.YES;

/**
 * Blocking array-based result consumer which delivers the results to API caller.
 */
public class BlockingRootResultConsumer implements RootResultConsumer {
    /** Mutex for concurrency support. */
    private final Object mux = new Object();

    /** Iterator over produced rows. */
    private final InternalIterator iterator = new InternalIterator();

    /**
     * {@link HasNextImmediatelyResult#RETRY} result from {@link
     * ResultIterator#hasNextImmediately()} is not allowed, the method will
     * block instead.
     */
    private final boolean waitForFullBatch;

    /** Query context to schedule root execution when the next batch is needed. */
    private volatile ScheduleCallback scheduleCallback;

    /** The batch that is currently being consumed. */
    private List<Row> currentBatch;

    /** When "true", no more batches are expected. */
    private boolean done;

    /** Error which occurred during query execution. */
    private QueryException doneError;

    /**
     * @param waitForFullBatch Disables {@link HasNextImmediatelyResult#RETRY} result
     *      from {@link ResultIterator#hasNextImmediately()}
     */
    // useMinimumLatency=true is used from Jet
    public BlockingRootResultConsumer(boolean waitForFullBatch) {
        this.waitForFullBatch = waitForFullBatch;
    }

    @Override
    public void setup(ScheduleCallback scheduleCallback) {
        this.scheduleCallback = scheduleCallback;
    }

    @Override
    public boolean consume(List<Row> batch, boolean last) {
        synchronized (mux) {
            if (done) {
                // this is possible if the query was concurrently cancelled
                // see https://github.com/hazelcast/hazelcast/issues/17160
                assert doneError != null;
                throw new RuntimeException(doneError);
            }

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
     * Poll the next batch from the upstream. If {@code shouldWait} is true, it
     * will block until a next batch is available or the end is reached. If
     * {@code shouldWait} is false, it will return a batch only if it's
     * immediately available, otherwise it will return {@code null}.
     *
     * @param shouldWait Enables blocking while waiting for the next batch.
     * @return The next batch or {@code null} if end of stream is reached or no
     *     batch is immediately available
     */
    private List<Row> getNextBatch(boolean shouldWait) {
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

                if (!shouldWait) {
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
        assert scheduleCallback != null;

        scheduleCallback.run();
    }

    @Override
    public ResultIterator<Row> iterator() {
        return iterator;
    }

    /**
     * Iterator over results.
     */
    private class InternalIterator implements ResultIterator<Row> {

        private List<Row> batch;
        private int position;

        @Override
        public boolean hasNext() {
            if (batch == null) {
                batch = getNextBatch(true);

                if (batch == null) {
                    assert done;

                    return false;
                }
            }

            return true;
        }

        @Override
        public HasNextImmediatelyResult hasNextImmediately() {
            if (batch == null) {
                batch = getNextBatch(waitForFullBatch);

                if (batch == null) {
                    return done ? DONE :  RETRY;
                }
            }

            return YES;
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
