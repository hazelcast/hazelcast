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

package com.hazelcast.ringbuffer.impl.client;

import com.hazelcast.ringbuffer.impl.operations.GenericOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.RingBufferPermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;

import static com.hazelcast.ringbuffer.impl.operations.GenericOperation.OPERATION_REMAINING_CAPACITY;

public class RemainingCapacityRequest extends RingbufferRequest {

    public RemainingCapacityRequest() {
    }

    public RemainingCapacityRequest(String name) {
        super(name);
    }

    @Override
    protected Operation prepareOperation() {
        return new GenericOperation(name, OPERATION_REMAINING_CAPACITY);
    }

    @Override
    public int getClassId() {
        return RingbufferPortableHook.REMAINING_CAPACITY;
    }

    @Override
    public Permission getRequiredPermission() {
        return new RingBufferPermission(name, ActionConstants.ACTION_READ);
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "remainingCapacity";
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }
}
