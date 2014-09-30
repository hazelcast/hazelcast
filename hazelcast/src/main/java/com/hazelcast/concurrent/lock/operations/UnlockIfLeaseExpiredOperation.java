/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.lock.operations;

import com.hazelcast.concurrent.lock.LockStoreImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.util.scheduler.EntryTaskScheduler;

import java.io.IOException;

public class UnlockIfLeaseExpiredOperation extends UnlockOperation {

    private final EntryTaskScheduler<Data, Object> scheduler;

    public UnlockIfLeaseExpiredOperation(ObjectNamespace namespace, Data key,
            EntryTaskScheduler<Data, Object> scheduler) {
        super(namespace, key, -1, true);
        this.scheduler = scheduler;
    }

    @Override
    public void run() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        long remainingLeaseTime = lockStore.getRemainingLeaseTime(key);
        if (remainingLeaseTime <= 0) {
            forceUnlock();
        } else {
            // reschedule lock eviction to be on safe side
            scheduler.schedule(remainingLeaseTime, key, null);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }
}
