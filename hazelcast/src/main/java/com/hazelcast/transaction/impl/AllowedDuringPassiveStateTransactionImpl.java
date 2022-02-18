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

package com.hazelcast.transaction.impl;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.impl.operations.CreateAllowedDuringPassiveStateTxBackupLogOperation;
import com.hazelcast.transaction.impl.operations.CreateTxBackupLogOperation;
import com.hazelcast.transaction.impl.operations.PurgeAllowedDuringPassiveStateTxBackupLogOperation;
import com.hazelcast.transaction.impl.operations.PurgeTxBackupLogOperation;
import com.hazelcast.transaction.impl.operations.ReplicateAllowedDuringPassiveStateTxBackupLogOperation;
import com.hazelcast.transaction.impl.operations.ReplicateTxBackupLogOperation;
import com.hazelcast.transaction.impl.operations.RollbackAllowedDuringPassiveStateTxBackupLogOperation;
import com.hazelcast.transaction.impl.operations.RollbackTxBackupLogOperation;

import java.util.List;
import java.util.UUID;

public class AllowedDuringPassiveStateTransactionImpl
        extends TransactionImpl {

    public AllowedDuringPassiveStateTransactionImpl(TransactionManagerServiceImpl transactionManagerService,
                                                    NodeEngine nodeEngine, TransactionOptions options, UUID txOwnerUuid) {
        super(transactionManagerService, nodeEngine, options, txOwnerUuid);
    }

    // used by tx backups
    AllowedDuringPassiveStateTransactionImpl(TransactionManagerServiceImpl transactionManagerService, NodeEngine nodeEngine,
                                             UUID txnId, List<TransactionLogRecord> transactionLog, long timeoutMillis,
                                             long startTime, UUID txOwnerUuid) {
        super(transactionManagerService, nodeEngine, txnId, transactionLog, timeoutMillis, startTime, txOwnerUuid);
    }

    protected CreateTxBackupLogOperation createCreateTxBackupLogOperation() {
        return new CreateAllowedDuringPassiveStateTxBackupLogOperation(getOwnerUuid(), getTxnId());
    }

    protected ReplicateTxBackupLogOperation createReplicateTxBackupLogOperation() {
        return new ReplicateAllowedDuringPassiveStateTxBackupLogOperation(
                getTransactionLog().getRecords(), getOwnerUuid(), getTxnId(), getTimeoutMillis(), getStartTime());
    }

    protected RollbackTxBackupLogOperation createRollbackTxBackupLogOperation() {
        return new RollbackAllowedDuringPassiveStateTxBackupLogOperation(getTxnId());
    }

    protected PurgeTxBackupLogOperation createPurgeTxBackupLogOperation() {
        return new PurgeAllowedDuringPassiveStateTxBackupLogOperation(getTxnId());
    }

}
