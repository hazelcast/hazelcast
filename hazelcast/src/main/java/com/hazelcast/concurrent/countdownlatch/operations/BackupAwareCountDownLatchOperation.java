/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.countdownlatch.operations;

import com.hazelcast.concurrent.countdownlatch.CountDownLatchContainer;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

abstract class BackupAwareCountDownLatchOperation extends BaseCountDownLatchOperation
        implements BackupAwareOperation {

    protected BackupAwareCountDownLatchOperation() {
    }

    public BackupAwareCountDownLatchOperation(String name) {
        super(name);
    }

    @Override
    public Operation getBackupOperation() {
        CountDownLatchService service = getService();
        CountDownLatchContainer latchContainer = service.getCountDownLatchContainer(name);
        int count = latchContainer != null ? latchContainer.getCount() : 0;
        return new CountDownLatchBackupOperation(name, count);
    }

    @Override
    public int getSyncBackupCount() {
        return 1;
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }
}
