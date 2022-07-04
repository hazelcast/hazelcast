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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorWaitNotifyKey;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import java.io.IOException;

public class ResultReadyNotifyOperation<V>
        extends AbstractSchedulerOperation
        implements Notifier {

    private ScheduledTaskHandler handler;

    public ResultReadyNotifyOperation() {
    }

    public ResultReadyNotifyOperation(ScheduledTaskHandler handler) {
        super(handler.getSchedulerName());
        this.handler = handler;
    }

    @Override
    public void run()
            throws Exception {
        // Ignore - Plain Notifier OP
    }

    @Override
    public boolean shouldNotify() {
        return true;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return new ScheduledExecutorWaitNotifyKey(getSchedulerName(), handler.toUrn());
    }

    @Override
    public int getClassId() {
        return ScheduledExecutorDataSerializerHook.PUBLISH_RESULT;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeString(handler.toUrn());
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.handler = ScheduledTaskHandler.of(in.readString());
    }
}
