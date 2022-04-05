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

package com.hazelcast.collection.impl.txncollection.operations;

import com.hazelcast.collection.impl.CollectionTxnUtil;
import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.impl.collection.operations.CollectionBackupAwareOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.List;

/**
 * a wrapper for running all commit operations at once
 */
public class CollectionCommitOperation extends CollectionBackupAwareOperation {

    private List<Operation> operationList;

    private transient List<Operation> backupList;

    public CollectionCommitOperation() {
    }

    public CollectionCommitOperation(int partitionId, String name, String serviceName, List<Operation> operationList) {
        super(name);
        setPartitionId(partitionId);
        setServiceName(serviceName);
        this.operationList = operationList;
    }

    @Override
    public void beforeRun() throws Exception {
        super.beforeRun();
        CollectionTxnUtil.before(operationList, this);
    }

    @Override
    public void run() throws Exception {
        backupList = CollectionTxnUtil.run(operationList);
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        CollectionTxnUtil.after(operationList);
    }

    @Override
    public boolean shouldBackup() {
        return !backupList.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new CollectionCommitBackupOperation(name, getServiceName(), backupList);
    }


    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.TXN_COMMIT;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        CollectionTxnUtil.write(out, operationList);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        operationList = CollectionTxnUtil.read(in);
    }
}
