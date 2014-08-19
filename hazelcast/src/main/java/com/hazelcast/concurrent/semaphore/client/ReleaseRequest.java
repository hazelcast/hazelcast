/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.semaphore.client;

import com.hazelcast.concurrent.semaphore.operations.ReleaseOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.SemaphorePermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;

public class ReleaseRequest extends SemaphoreRequest {

    public ReleaseRequest() {
    }

    public ReleaseRequest(String name, int permitCount) {
        super(name, permitCount);
    }

    @Override
    protected Operation prepareOperation() {
        return new ReleaseOperation(name, permitCount);
    }

    @Override
    public int getClassId() {
        return SemaphorePortableHook.RELEASE;
    }

    @Override
    public Permission getRequiredPermission() {
        return new SemaphorePermission(name, ActionConstants.ACTION_RELEASE);
    }

    @Override
    public String getMethodName() {
        return "release";
    }

    @Override
    public Object[] getParameters() {
        if (permitCount == -1) {
            return null;
        }
        return super.getParameters();
    }
}
