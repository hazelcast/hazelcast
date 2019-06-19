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

package com.hazelcast.cp.internal.datastructures.unsafe.countdownlatch.operations;

import com.hazelcast.cp.internal.datastructures.unsafe.countdownlatch.CountDownLatchContainer;
import com.hazelcast.cp.internal.datastructures.unsafe.countdownlatch.CountDownLatchService;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

abstract class BackupAwareCountDownLatchOperation extends AbstractCountDownLatchOperation
        implements BackupAwareOperation {

    protected BackupAwareCountDownLatchOperation() {
    }

    protected BackupAwareCountDownLatchOperation(String name) {
        super(name);
    }

    @Override
    public Operation getBackupOperation() {
        CountDownLatchService service = getService();
        CountDownLatchContainer container = service.getCountDownLatchContainer(name);
        int count = container != null ? container.getCount() : 0;
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
