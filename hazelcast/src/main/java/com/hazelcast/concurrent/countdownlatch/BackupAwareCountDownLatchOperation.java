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

package com.hazelcast.concurrent.countdownlatch;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

/**
 * @mdogan 1/10/13
 */
abstract class BackupAwareCountDownLatchOperation extends BaseCountDownLatchOperation implements BackupAwareOperation {

    protected BackupAwareCountDownLatchOperation() {
    }

    public BackupAwareCountDownLatchOperation(String name) {
        super(name);
    }

    public Operation getBackupOperation() {
        CountDownLatchService service = getService();
        CountDownLatchInfo latch = service.getLatch(name);
        final int count = latch != null ? latch.getCount() : 0;
        final String owner = latch != null ? latch.getOwner() : null;
        return new CountDownLatchBackupOperation(name, count, owner);
    }

    public int getSyncBackupCount() {
        return 1;
    }

    public int getAsyncBackupCount() {
        return 0;
    }

}
