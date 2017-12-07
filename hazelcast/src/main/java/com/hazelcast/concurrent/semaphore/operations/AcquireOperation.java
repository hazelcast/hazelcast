/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.concurrent.semaphore.SemaphoreContainer;
import com.hazelcast.concurrent.semaphore.SemaphoreDataSerializerHook;
import com.hazelcast.concurrent.semaphore.SemaphoreWaitNotifyKey;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.impl.MutatingOperation;

import static java.lang.Boolean.TRUE;

public class AcquireOperation extends SemaphoreBackupAwareOperation implements BlockingOperation, MutatingOperation {

    public AcquireOperation() {
    }

    public AcquireOperation(String name, int permitCount, long timeout) {
        super(name, permitCount);
        setWaitTimeout(timeout);
    }

    @Override
    public void run() throws Exception {
        SemaphoreContainer semaphoreContainer = getSemaphoreContainer();
        response = semaphoreContainer.acquire(getCallerUuid(), permitCount);
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new SemaphoreWaitNotifyKey(name, "acquire");
    }

    @Override
    public boolean shouldWait() {
        SemaphoreContainer semaphoreContainer = getSemaphoreContainer();
        return getWaitTimeout() != 0 && !semaphoreContainer.isAvailable(permitCount);
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
    }

    @Override
    public boolean shouldBackup() {
        return TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new AcquireBackupOperation(name, permitCount, getCallerUuid());
    }

    @Override
    public int getId() {
        return SemaphoreDataSerializerHook.ACQUIRE_OPERATION;
    }
}
