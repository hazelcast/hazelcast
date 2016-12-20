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

public abstract class AsyncExecutionOperation extends EngineOperation {

    protected long executionId;

    protected AsyncExecutionOperation() {
    }

    public AsyncExecutionOperation(String engineName, long executionId) {
        super(engineName);
        this.executionId = executionId;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public Object getResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void beforeRun() throws Exception {
        this.<JetService>getService().registerOperation(this);
    }

    @Override
    public final void run() throws Exception {
        try {
            doRun();
        } catch (Exception e) {
            logError(e);
            doSendResponse(e);
        }
    }

    abstract void cancel();

    abstract void completeExceptionally(Throwable throwable);

    protected abstract void doRun() throws Exception;

    public long getExecutionId() {
        return executionId;
    }

    protected final void doSendResponse(Object value) {
        try {
            sendResponse(value);
        } finally {
            this.<JetService>getService().deregisterOperation(this);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(executionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        executionId = in.readLong();
    }
}
