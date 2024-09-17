/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl;

import com.hazelcast.collection.impl.txncollection.CollectionTxnOperation;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public final class CollectionTxnUtil {

    private CollectionTxnUtil() {
    }

    /**
     * @param operation the operation.
     * @return negative itemId if the operation is a remove operation
     */
    public static long getItemId(CollectionTxnOperation operation) {
        int pollOperation = operation.isRemoveOperation() ? 1 : -1;
        return pollOperation * operation.getItemId();
    }

    /**
     * @param itemId the item ID
     * @return true if itemId is greater than 0
     */
    public static boolean isRemove(long itemId) {
        return itemId > 0 ;
    }

    public static void before(List<Operation> operationList, Operation wrapper) throws Exception {
        for (Operation operation : operationList) {
            operation.setService(wrapper.getService());
            operation.setServiceName(wrapper.getServiceName());
            operation.setCallerUuid(wrapper.getCallerUuid());
            operation.setNodeEngine(wrapper.getNodeEngine());
            operation.setPartitionId(wrapper.getPartitionId());
            operation.beforeRun();
        }
    }

    public static List<Operation> run(List<Operation> operationList) throws Exception {
        List<Operation> backupList = new LinkedList<>();
        for (Operation operation : operationList) {
            operation.run();
            if (operation instanceof BackupAwareOperation backupAwareOperation) {
                if (backupAwareOperation.shouldBackup()) {
                    backupList.add(backupAwareOperation.getBackupOperation());
                }
            }
        }
        return backupList;
    }

    public static void after(List<Operation> operationList) throws Exception {
        for (Operation operation : operationList) {
            operation.afterRun();
        }
    }

    public static void write(ObjectDataOutput out, List<Operation> operationList) throws IOException {
        SerializationUtil.writeList(operationList, out);
    }

    public static List<Operation> read(ObjectDataInput in) throws IOException {
        return SerializationUtil.readList(in);
    }
}
