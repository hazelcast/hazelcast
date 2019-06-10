/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations;

import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.SemaphoreContainer;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.SemaphoreDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.SemaphoreService;

public class SemaphoreDetachMemberBackupOperation extends SemaphoreBackupOperation {

    public SemaphoreDetachMemberBackupOperation() {
    }

    public SemaphoreDetachMemberBackupOperation(String name, String firstCaller) {
        super(name, -1, firstCaller);
    }

    @Override
    public void run() throws Exception {
        SemaphoreService service = getService();
        if (service.containsSemaphore(name)) {
            SemaphoreContainer semaphoreContainer = service.getSemaphoreContainer(name);
            response = semaphoreContainer.detachAll(firstCaller);
        }
    }

    @Override
    public int getClassId() {
        return SemaphoreDataSerializerHook.DETACH_MEMBER_BACKUP_OPERATION;
    }
}
