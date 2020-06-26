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

import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;

public class FaultyExec extends AbstractExec {

    private final Exec exec;
    private final RuntimeException error;

    public FaultyExec(Exec exec, RuntimeException error) {
        super(exec.getId());

        this.exec = exec;
        this.error = error;
    }

    @Override
    protected void setup0(QueryFragmentContext ctx) {
        super.setup0(ctx);
    }

    @Override
    protected IterationResult advance0() {
        throw error;
    }

    @Override
    protected RowBatch currentBatch0() {
        return exec.currentBatch();
    }
}
