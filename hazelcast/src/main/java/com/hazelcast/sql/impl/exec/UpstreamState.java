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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

import java.util.Iterator;

import static com.hazelcast.sql.impl.exec.IterationResult.FETCHED_DONE;
import static com.hazelcast.sql.impl.exec.IterationResult.WAIT;

/**
 * Upstream state.
 */
public class UpstreamState implements Iterable<Row> {
    /** Upstream operator. */
    private final Exec upstream;

    /** Iterator over the current batch. */
    private final UpstreamIterator iter;

    /** Current batch returned from the upstream. */
    private RowBatch currentBatch = EmptyRowBatch.INSTANCE;

    /** Current position. */
    private int currentBatchPos;

    /** Last returned state. */
    private IterationResult state;

    public UpstreamState(Exec upstream) {
        this.upstream = upstream;

        iter = new UpstreamIterator();
    }

    /**
     * Try advancing the upstream.
     *
     * @return {@code true} if the caller may try iteration over results; {@code false} if the caller should give
     * up execution and wait.
     */
    public boolean advance() {
        // If some data is available still, do not do anything, just return the previous result.
        if (isNextAvailable()) {
            return true;
        }

        // If the upstream is exhausted, just return "done" flag.
        if (state == FETCHED_DONE) {
            return true;
        }

        // Otherwise poll the upstream.
        state = upstream.advance();

        switch (state) {
            case FETCHED_DONE:
            case FETCHED:
                currentBatch = upstream.currentBatch();
                assert currentBatch != null;

                currentBatchPos = 0;

                return true;

            default:
                assert state == WAIT;

                currentBatch = EmptyRowBatch.INSTANCE;
                currentBatchPos = 0;

                return false;
        }
    }

    public void setup(QueryFragmentContext ctx) {
        upstream.setup(ctx);
    }

    public RowBatch consumeBatch() {
        if (currentBatchPos != 0) {
            throw QueryException.error("Batch can be consumed only as a whole: " + upstream);
        }

        RowBatch batch = currentBatch;

        currentBatchPos = batch.getRowCount();

        return batch;
    }

    /**
     * @return {@code true} if no more results will appear in future.
     */
    public boolean isDone() {
        return state == FETCHED_DONE && !iter.hasNext();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Iterator<Row> iterator() {
        return iter;
    }

    /**
     * Return next row in the current batch if it exists.
     *
     * @return Next row or {@code null}.
     */
    public Row nextIfExists() {
        return isNextAvailable() ? iter.next() : null;
    }

    /**
     * @return {@code true} if the next row is available without the need to advance the upstream.
     */
    public boolean isNextAvailable() {
        return currentBatchPos < currentBatch.getRowCount();
    }

    Exec getUpstream() {
        return upstream;
    }

    /**
     * Iterator over current upstream batch.
     */
    private class UpstreamIterator implements Iterator<Row> {
        @Override
        public boolean hasNext() {
            return isNextAvailable();
        }

        @Override
        public Row next() {
            return currentBatch.getRow(currentBatchPos++);
        }
    }
}
