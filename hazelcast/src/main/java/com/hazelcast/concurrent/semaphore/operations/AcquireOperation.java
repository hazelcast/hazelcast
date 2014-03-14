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

package com.hazelcast.concurrent.semaphore.operations;

import com.hazelcast.concurrent.semaphore.Permit;
import com.hazelcast.concurrent.semaphore.SemaphoreDataSerializerHook;
import com.hazelcast.concurrent.semaphore.SemaphoreWaitNotifyKey;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;

public class AcquireOperation extends SemaphoreBackupAwareOperation
        implements WaitSupport, IdentifiedDataSerializable {

    public AcquireOperation() {
    }

    public AcquireOperation(String name, int permitCount, long timeout) {
        super(name, permitCount);
        setWaitTimeout(timeout);
    }

    @Override
    public void run() throws Exception {
        Permit permit = getPermit();
        response = permit.acquire(permitCount, getCallerUuid());
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new SemaphoreWaitNotifyKey(name, "acquire");
    }

    @Override
    public boolean shouldWait() {
        Permit permit = getPermit();
        return getWaitTimeout() != 0 && !permit.isAvailable(permitCount);
    }

    @Override
    public void onWaitExpire() {
        ResponseHandler responseHandler = getResponseHandler();
        responseHandler.sendResponse(false);
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new AcquireBackupOperation(name, permitCount, getCallerUuid());
    }

    @Override
    public int getFactoryId() {
        return SemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SemaphoreDataSerializerHook.ACQUIRE_OPERATION;
    }
}
