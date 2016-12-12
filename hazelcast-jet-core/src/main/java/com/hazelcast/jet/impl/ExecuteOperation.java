/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

class ExecuteOperation extends AsyncOperation {

    private long planId;
    private volatile CompletionStage<Void> executionFuture;

    ExecuteOperation(String engineName, long executionId) {
        super(engineName);
        this.planId = executionId;
    }

    private ExecuteOperation() {
        // for deserialization
    }

    @Override
    void cancel() {
        if (executionFuture != null) {
            executionFuture.toCompletableFuture().cancel(true);
        }
    }

    @Override
    protected void doRun() throws Exception {
        JetService service = getService();
        EngineContext engineContext = service.getEngineContext(engineName);
        executionFuture = engineContext.getExecutionContext(planId)
                                       .execute(f -> f.handle((r, error) -> error != null ? error : null)
                                                      .thenAccept(this::doSendResponse));
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(planId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        planId = in.readLong();
    }
}
