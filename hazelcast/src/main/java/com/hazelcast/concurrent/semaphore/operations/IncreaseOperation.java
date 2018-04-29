/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.impl.MutatingOperation;

public class IncreaseOperation extends SemaphoreBackupAwareOperation implements MutatingOperation {

    public IncreaseOperation() {
    }

    public IncreaseOperation(String name, int permitCount) {
        super(name, permitCount);
    }

    @Override
    public void run() throws Exception {
        SemaphoreContainer semaphoreContainer = getSemaphoreContainer();
        response = semaphoreContainer.increase(permitCount);
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new IncreaseBackupOperation(name, permitCount);
    }

    @Override
    public int getId() {
        return SemaphoreDataSerializerHook.INCREASE_OPERATION;
    }
}
