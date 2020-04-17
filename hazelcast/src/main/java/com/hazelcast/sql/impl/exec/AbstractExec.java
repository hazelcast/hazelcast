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

import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.RowBatch;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Abstract executor.
 */
@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public abstract class AbstractExec implements Exec {

    protected QueryFragmentContext ctx;
    private final int id;
    private boolean done;

    protected AbstractExec(int id) {
        this.id = id;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public final void setup(QueryFragmentContext ctx) {
        this.ctx = ctx;

        setup0(ctx);
    }

    @Override
    public final IterationResult advance() {
        checkCancelled();

        if (done) {
            throw new IllegalStateException("Iteration is finished.");
        }

        IterationResult res = advance0();

        if (res == IterationResult.FETCHED_DONE) {
            done = true;
        }

        return res;
    }

    @Override
    public final RowBatch currentBatch() {
        RowBatch res = currentBatch0();

        return res != null ? res : EmptyRowBatch.INSTANCE;
    }

    protected void setup0(QueryFragmentContext ctx) {
        // No-op.
    }

    protected abstract IterationResult advance0();

    protected abstract RowBatch currentBatch0();

    protected void checkCancelled() {
        ctx.checkCancelled();
    }
}
