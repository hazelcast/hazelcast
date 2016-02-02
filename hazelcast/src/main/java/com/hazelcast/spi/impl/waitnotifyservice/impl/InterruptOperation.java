/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.waitnotifyservice.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.waitnotifyservice.InternalWaitNotifyService;

import java.io.IOException;

/**
 * An {@link com.hazelcast.spi.Operation} responsible for interrupting a {@link com.hazelcast.spi.WaitSupport} operation.
 *
 * This is done by invoking the {@link InternalWaitNotifyService#interrupt(WaitNotifyKey, String, long)} method.
 */
public class InterruptOperation extends AbstractOperation implements IdentifiedDataSerializable {

    private WaitNotifyKey waitNotifyKey;

    // the callId of the operation to interrupt.
    private long targetCallId;

    public InterruptOperation() {
    }

    public InterruptOperation(WaitNotifyKey waitNotifyKey, long targetCallId) {
        this.waitNotifyKey = waitNotifyKey;
        this.targetCallId = targetCallId;
    }

    @Override
    public void run() throws Exception {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        InternalWaitNotifyService waitNotifyService = nodeEngine.getWaitNotifyService();
        waitNotifyService.interrupt(waitNotifyKey, getCallerUuid(), targetCallId);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SpiDataSerializerHook.INTERRUPT_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(waitNotifyKey);
        out.writeLong(targetCallId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        waitNotifyKey = in.readObject();
        targetCallId = in.readLong();
    }
}
