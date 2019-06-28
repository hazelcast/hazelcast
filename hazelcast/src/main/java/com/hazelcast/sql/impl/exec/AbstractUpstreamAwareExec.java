/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.sql.impl.worker.data.DataWorker;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.RowBatch;

/**
 * Executor which has an upstream executor and hence delegate to it at some stages.
 */
public abstract class AbstractUpstreamAwareExec extends AbstractExec {
    /** Upstream operator. */
    private final Exec upstream;

    /** Current batch returned from the upstream. */
    protected RowBatch upstreamCurrentBatch;

    /** Whether upstream is finished. */
    protected boolean upstreamDone;

    /**
     * Constructor.
     *
     * @param upstream Upstream stage.
     */
    protected AbstractUpstreamAwareExec(Exec upstream) {
        this.upstream = upstream;
    }

    @Override
    protected final void setup0(QueryContext ctx, DataWorker worker) {
        upstream.setup(ctx, worker);

        setup1(ctx, worker);
    }

    protected void setup1(QueryContext ctx, DataWorker worker) {
        // No-op.
    }

    /**
     * Advance upstream stage and return the result.
     *
     * @return Advance result.
     */
    protected IterationResult advanceUpstream() {
        if (upstreamDone)
            return IterationResult.FETCHED_DONE;

        IterationResult res = upstream.advance();

        switch (res) {
            case FETCHED_DONE:
                upstreamDone = true;

                // Fall-through.

            case FETCHED:
                upstreamCurrentBatch = upstream.currentBatch();

                break;

            case WAIT:
                upstreamCurrentBatch = EmptyRowBatch.INSTANCE;

                break;

            default:
                throw new IllegalStateException("Should not reach this.");
        }

        return res;
    }
}
