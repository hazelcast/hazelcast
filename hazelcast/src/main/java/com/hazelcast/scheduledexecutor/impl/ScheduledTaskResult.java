/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

public class ScheduledTaskResult
        implements IdentifiedDataSerializable {

    private boolean done;

    private Object result;

    private Throwable exception;

    private boolean cancelled;

    ScheduledTaskResult() {
    }

    ScheduledTaskResult(boolean cancelled) {
        this.cancelled = cancelled;
    }

    ScheduledTaskResult(Throwable exception) {
        this.exception = exception;
    }

    ScheduledTaskResult(Object result) {
        this.result = result;
        this.done = true;
    }

    public Object getReturnValue() {
        return result;
    }

    public Throwable getException() {
        return exception;
    }

    boolean wasCancelled() {
        return cancelled;
    }

    void checkErroneousState() {
        if (wasCancelled()) {
            throw new CancellationException();
        } else if (exception != null) {
            throw new ExecutionExceptionDecorator(new ExecutionException(exception));
        }
    }

    @Override
    public int getFactoryId() {
        return ScheduledExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ScheduledExecutorDataSerializerHook.TASK_RESOLUTION;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeObject(result);
        out.writeBoolean(done);
        out.writeBoolean(cancelled);
        out.writeObject(exception);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        result = in.readObject();
        done = in.readBoolean();
        cancelled = in.readBoolean();
        exception = in.readObject();
    }

    @Override
    public String toString() {
        return "ScheduledTaskResult{"
                + "result=" + result
                + ", exception=" + exception
                + ", cancelled=" + cancelled
                + '}';
    }

    // ExecutionExceptions get peeled away during Operation response, this wrapper
    // helps identifying them on the proxy and re-construct them.
    public static class ExecutionExceptionDecorator
            extends RuntimeException {

        public ExecutionExceptionDecorator(Throwable cause) {
            super(cause);
        }
    }
}
