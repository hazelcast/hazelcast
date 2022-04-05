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

package com.hazelcast.scheduledexecutor.impl.operations;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorWaitNotifyKey;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import java.io.IOException;

public class GetResultOperation<V>
        extends AbstractSchedulerOperation
        implements BlockingOperation, ReadonlyOperation {

    private String taskName;

    private ScheduledTaskHandler handler;

    private Object result;

    public GetResultOperation() {
    }

    public GetResultOperation(ScheduledTaskHandler handler) {
        super(handler.getSchedulerName());
        this.taskName = handler.getTaskName();
        this.handler = handler;
    }

    @Override
    public void run()
            throws Exception {
        result = getContainer().get(taskName);
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new ScheduledExecutorWaitNotifyKey(getSchedulerName(), handler.toUrn());
    }

    @Override
    public boolean shouldWait() {
        return getContainer().shouldParkGetResult(taskName);
    }

    @Override
    public void onWaitExpire() {
        sendResponse(new HazelcastException());
    }

    @Override
    public int getClassId() {
        return ScheduledExecutorDataSerializerHook.GET_RESULT;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeString(taskName);
        out.writeString(handler.toUrn());
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.taskName = in.readString();
        this.handler = ScheduledTaskHandler.of(in.readString());
    }
}
