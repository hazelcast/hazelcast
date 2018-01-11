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
import com.hazelcast.spi.BackupAwareOperation;

public abstract class SemaphoreBackupAwareOperation extends SemaphoreOperation implements BackupAwareOperation {

    protected SemaphoreBackupAwareOperation() {
    }

    protected SemaphoreBackupAwareOperation(String name, int permitCount) {
        super(name, permitCount);
    }

    @Override
    public int getAsyncBackupCount() {
        SemaphoreContainer semaphoreContainer = getSemaphoreContainer();
        return semaphoreContainer.getAsyncBackupCount();
    }

    @Override
    public int getSyncBackupCount() {
        SemaphoreContainer semaphoreContainer = getSemaphoreContainer();
        return semaphoreContainer.getSyncBackupCount();
    }
}
