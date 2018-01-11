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

package com.hazelcast.concurrent.lock.operations;

import com.hazelcast.concurrent.lock.LockDataSerializerHook;
import com.hazelcast.concurrent.lock.LockServiceImpl;
import com.hazelcast.concurrent.lock.LockStoreContainer;
import com.hazelcast.concurrent.lock.LockStoreImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ServiceNamespace;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

public class LockReplicationOperation extends Operation
        implements IdentifiedDataSerializable, Versioned {

    private final Collection<LockStoreImpl> locks = new LinkedList<LockStoreImpl>();

    public LockReplicationOperation() {
    }

    public LockReplicationOperation(LockStoreContainer container, int partitionId, int replicaIndex) {
        this(container, partitionId, replicaIndex, container.getAllNamespaces(replicaIndex));
    }

    public LockReplicationOperation(LockStoreContainer container, int partitionId, int replicaIndex,
            Collection<ServiceNamespace> namespaces) {

        setPartitionId(partitionId).setReplicaIndex(replicaIndex);

        for (ServiceNamespace namespace : namespaces) {
            LockStoreImpl ls = container.getLockStore((ObjectNamespace) namespace);
            if (ls == null) {
                continue;
            }
            if (ls.getTotalBackupCount() < replicaIndex) {
                continue;
            }

            locks.add(ls);
        }
    }

    @Override
    public void run() {
        LockServiceImpl lockService = getService();
        LockStoreContainer container = lockService.getLockContainer(getPartitionId());
        for (LockStoreImpl ls : locks) {
            container.put(ls);
        }
    }

    @Override
    public String getServiceName() {
        return LockServiceImpl.SERVICE_NAME;
    }

    public boolean isEmpty() {
        return locks.isEmpty();
    }

    @Override
    public int getFactoryId() {
        return LockDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return LockDataSerializerHook.LOCK_REPLICATION;
    }

    @Override
    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        int len = locks.size();
        out.writeInt(len);
        if (len > 0) {
            for (LockStoreImpl ls : locks) {
                ls.writeData(out);
            }
        }
    }

    @Override
    protected void readInternal(final ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int len = in.readInt();
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                LockStoreImpl ls = new LockStoreImpl();
                ls.readData(in);
                locks.add(ls);
            }
        }
    }
}
