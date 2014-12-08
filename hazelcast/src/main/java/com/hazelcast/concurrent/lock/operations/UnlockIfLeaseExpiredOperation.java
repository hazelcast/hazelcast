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
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ObjectNamespace;

import java.io.IOException;

public final class UnlockIfLeaseExpiredOperation extends UnlockOperation {

    private final int version;

    public UnlockIfLeaseExpiredOperation(ObjectNamespace namespace, Data key, int version) {
        super(namespace, key, -1, true);
        this.version = version;
    }

    @Override
    public void run() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        int lockVersion = lockStore.getVersion(key);
        if (version == lockVersion) {
            forceUnlock();
        } else {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("Won't unlock since lock version is not matching expiration version: "
                        + lockVersion + " vs " + version);
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException("This operation is intended to be executed on local member only!");
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException("This operation is intended to be executed on local member only!");
    }
}
