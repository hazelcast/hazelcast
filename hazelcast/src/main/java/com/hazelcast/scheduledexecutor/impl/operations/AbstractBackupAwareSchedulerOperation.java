/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor.impl.operations;

import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;

public abstract class AbstractBackupAwareSchedulerOperation
        extends AbstractSchedulerOperation
        implements BackupAwareOperation {

    AbstractBackupAwareSchedulerOperation() {
    }

    AbstractBackupAwareSchedulerOperation(String schedulerName) {
        super(schedulerName);
    }

    @Override
    public boolean shouldBackup() {
        boolean isMemberOperation = getPartitionId() == -1;
        return !isMemberOperation;
    }

    @Override
    public int getSyncBackupCount() {
        return getContainer().getDurability();
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

}

