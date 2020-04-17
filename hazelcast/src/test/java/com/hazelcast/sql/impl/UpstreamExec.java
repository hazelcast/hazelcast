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

import com.hazelcast.sql.impl.exec.AbstractExec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.row.RowBatch;

import java.util.ArrayDeque;

/**
 * Convenient upstream executor that produces required results.
 */
public class UpstreamExec extends AbstractExec {

    private final ArrayDeque<UpstreamResult> results = new ArrayDeque<>();
    private UpstreamResult currentResult;

    public UpstreamExec(int id) {
        super(id);
    }

    @Override
    protected IterationResult advance0() {
        currentResult = results.poll();

        if (currentResult == null) {
            return IterationResult.WAIT;
        }

        throwErrorIfNeeded();

        return currentResult.getResult();
    }

    @Override
    protected RowBatch currentBatch0() {
        throwErrorIfNeeded();

        return currentResult.getBatch();
    }

    public void addResult(IterationResult result, RowBatch batch) {
        results.add(new UpstreamResult(result, batch, null));
    }

    public void addError(RuntimeException error) {
        results.add(new UpstreamResult(null, null, error));
    }

    private void throwErrorIfNeeded() {
        RuntimeException error = currentResult.getError();

        if (error != null) {
            throw error;
        }
    }

    private static final class UpstreamResult {
        private final IterationResult result;
        private final RowBatch batch;
        private final RuntimeException error;

        private UpstreamResult(IterationResult result, RowBatch batch, RuntimeException error) {
            this.result = result;
            this.batch = batch;
            this.error = error;
        }

        private IterationResult getResult() {
            return result;
        }

        private RowBatch getBatch() {
            return batch;
        }

        public RuntimeException getError() {
            return error;
        }
    }
}
