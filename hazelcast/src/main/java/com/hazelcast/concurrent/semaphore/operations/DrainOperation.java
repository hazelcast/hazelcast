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

package com.hazelcast.concurrent.semaphore.operations;

import com.hazelcast.concurrent.semaphore.Permit;
import com.hazelcast.concurrent.semaphore.SemaphoreDataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;

public class DrainOperation extends SemaphoreBackupAwareOperation implements IdentifiedDataSerializable {

    public DrainOperation() {
    }

    public DrainOperation(String name) {
        super(name, -1);
    }

    @Override
    public void run() throws Exception {
        Permit permit = getPermit();
        response = permit.drain(getCallerUuid());
    }

    @Override
    public boolean shouldBackup() {
        return !response.equals(0);
    }

    @Override
    public Operation getBackupOperation() {
        return new DrainBackupOperation(name, permitCount, getCallerUuid());
    }

    @Override
    public int getFactoryId() {
        return SemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SemaphoreDataSerializerHook.DRAIN_OPERATION;
    }
}
