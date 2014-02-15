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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class DeadMemberBackupOperation extends SemaphoreBackupOperation
        implements IdentifiedDataSerializable {

    public DeadMemberBackupOperation() {
    }

    public DeadMemberBackupOperation(String name, String firstCaller) {
        super(name, -1, firstCaller);
    }

    @Override
    public void run() throws Exception {
        Permit permit = getPermit();
        permit.memberRemoved(firstCaller);
        response = true;
    }

    @Override
    public int getFactoryId() {
        return SemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SemaphoreDataSerializerHook.DEAD_MEMBER_BACKUP_OPERATION;
    }
}
