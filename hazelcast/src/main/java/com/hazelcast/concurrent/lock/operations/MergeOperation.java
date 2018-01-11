/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.concurrent.lock.LockDataSerializerHook;
import com.hazelcast.concurrent.lock.LockResourceImpl;
import com.hazelcast.concurrent.lock.LockServiceImpl;
import com.hazelcast.concurrent.lock.LockStoreContainer;
import com.hazelcast.concurrent.lock.LockStoreImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.ReadonlyOperation;

import java.io.IOException;

public class MergeOperation extends AbstractLockOperation implements ReadonlyOperation {

    private LockResourceImpl lockResource;

    public MergeOperation() {
    }

    public MergeOperation(LockStoreImpl lockStore) {
        super(lockStore.getNamespace(), null, -1);
    }

    public MergeOperation(ObjectNamespace namespace, Data key, LockResourceImpl lockResource) {
        super(namespace, key, -1);
        this.lockResource = lockResource;
    }

    @Override
    public int getId() {
        return LockDataSerializerHook.MERGE;
    }

    @Override
    public void run() {
        LockServiceImpl service = getService();
        LockStoreContainer lockContainer = service.getLockContainer(getPartitionId());
        LockStoreImpl lockStore = lockContainer.getLockStore(namespace);
        if (lockStore != null) {
            System.err.println("Lock " + namespace.getObjectName()  + " already exists!");
            response = false;
            return;
        }
        System.err.println("Lock " + namespace.getObjectName()  + " doesn't exist, we're free to create it!");
        getLockStore().init(lockResource);
        response = true;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(lockResource);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        lockResource = in.readObject();
    }
}
