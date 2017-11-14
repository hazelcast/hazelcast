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
import com.hazelcast.spi.Operation;

public class DrainOperation extends SemaphoreBackupAwareOperation {

    private transient int response;

    public DrainOperation() {
    }

    public DrainOperation(String name) {
        super(name, -1);
    }

    @Override
    public Integer call() throws Exception {
        SemaphoreContainer semaphoreContainer = getSemaphoreContainer();
        response = semaphoreContainer.drain(getCallerUuid());
        return response;
    }

    @Override
    public boolean shouldBackup() {
        return response != 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new DrainBackupOperation(name, permitCount, getCallerUuid());
    }

    @Override
    public int getId() {
        return SemaphoreDataSerializerHook.DRAIN_OPERATION;
    }
}
