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

package com.hazelcast.cp.internal.datastructures.semaphore.operation;

import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.datastructures.semaphore.Semaphore;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;

/**
 * Operation for {@link ISemaphore#increasePermits(int)}
 * and {@link ISemaphore#reducePermits(int)}
 *
 * @see Semaphore#change(long, long, UUID, int)
 */
public class ChangePermitsOp extends AbstractSemaphoreOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    private int permits;

    public ChangePermitsOp() {
    }

    public ChangePermitsOp(String name, long sessionId, long threadId, UUID invocationUid, int permits) {
        super(name, sessionId, threadId, invocationUid);
        this.permits = permits;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        SemaphoreService service = getService();
        return service.changePermits(groupId, commitIndex, name, getSemaphoreEndpoint(), invocationUid, permits);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return sessionId != NO_SESSION_ID;
    }

    @Override
    protected String getServiceName() {
        return SemaphoreService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return SemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SemaphoreDataSerializerHook.CHANGE_PERMITS_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(permits);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        permits = in.readInt();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", permits=").append(permits);
    }
}
