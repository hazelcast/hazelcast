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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Converts an operation class into something readable.
 * <p>
 * In most cases the class name is sufficient, but there are certain
 * operations like {@link Backup} and {@link PartitionIteratingOperation} where
 * one needs to see inside the content of an operation.
 */
public final class OperationDescriptors {

    // the key is the name of the class as string, to prevent any class references being retained
    private static final ConcurrentMap<String, String> DESCRIPTORS = new ConcurrentHashMap<String, String>();

    private OperationDescriptors() {
    }

    public static String toOperationDesc(Operation op) {
        Class<? extends Operation> operationClass = op.getClass();
        if (PartitionIteratingOperation.class.isAssignableFrom(operationClass)) {
            PartitionIteratingOperation partitionIteratingOperation = (PartitionIteratingOperation) op;
            OperationFactory operationFactory = partitionIteratingOperation.getOperationFactory();
            String desc = DESCRIPTORS.get(operationFactory.getClass().getName());
            if (desc == null) {
                desc = PartitionIteratingOperation.class.getSimpleName() + "(" + operationFactory.getClass().getName() + ")";
                DESCRIPTORS.put(operationFactory.getClass().getName(), desc);
            }
            return desc;
        } else if (Backup.class.isAssignableFrom(operationClass)) {
            Backup backup = (Backup) op;
            Operation backupOperation = backup.getBackupOp();
            String desc = DESCRIPTORS.get(backupOperation.getClass().getName());
            if (desc == null) {
                desc = Backup.class.getSimpleName() + "(" + backup.getBackupOp().getClass().getName() + ")";
                DESCRIPTORS.put(backupOperation.getClass().getName(), desc);
            }
            return desc;
        } else {
            return operationClass.getName();
        }
    }
}
