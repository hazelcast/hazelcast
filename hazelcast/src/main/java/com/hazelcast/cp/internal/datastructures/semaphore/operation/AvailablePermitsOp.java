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
import com.hazelcast.cp.internal.datastructures.semaphore.Semaphore;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.RaftOp;

import java.io.IOException;

/**
 * Operation for {@link ISemaphore#availablePermits()}
 *
 * @see Semaphore#getAvailable()
 */
public class AvailablePermitsOp extends RaftOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    private String name;

    public AvailablePermitsOp() {
    }

    public AvailablePermitsOp(String name) {
        this.name = name;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        SemaphoreService service = getService();
        return service.availablePermits(groupId, name);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
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
        return SemaphoreDataSerializerHook.AVAILABLE_PERMITS_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", name=").append(name);
    }
}
