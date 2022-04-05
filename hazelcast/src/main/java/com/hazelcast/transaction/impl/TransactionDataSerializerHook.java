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

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.transaction.impl.operations.BroadcastTxRollbackOperation;
import com.hazelcast.transaction.impl.operations.CreateAllowedDuringPassiveStateTxBackupLogOperation;
import com.hazelcast.transaction.impl.operations.CreateTxBackupLogOperation;
import com.hazelcast.transaction.impl.operations.PurgeAllowedDuringPassiveStateTxBackupLogOperation;
import com.hazelcast.transaction.impl.operations.PurgeTxBackupLogOperation;
import com.hazelcast.transaction.impl.operations.ReplicateAllowedDuringPassiveStateTxBackupLogOperation;
import com.hazelcast.transaction.impl.operations.ReplicateTxBackupLogOperation;
import com.hazelcast.transaction.impl.operations.RollbackAllowedDuringPassiveStateTxBackupLogOperation;
import com.hazelcast.transaction.impl.operations.RollbackTxBackupLogOperation;
import com.hazelcast.transaction.impl.xa.XATransactionDTO;
import com.hazelcast.transaction.impl.xa.operations.ClearRemoteTransactionBackupOperation;
import com.hazelcast.transaction.impl.xa.operations.ClearRemoteTransactionOperation;
import com.hazelcast.transaction.impl.xa.operations.CollectRemoteTransactionsOperation;
import com.hazelcast.transaction.impl.xa.operations.FinalizeRemoteTransactionBackupOperation;
import com.hazelcast.transaction.impl.xa.operations.FinalizeRemoteTransactionOperation;
import com.hazelcast.transaction.impl.xa.operations.PutRemoteTransactionBackupOperation;
import com.hazelcast.transaction.impl.xa.operations.PutRemoteTransactionOperation;
import com.hazelcast.transaction.impl.xa.operations.XaReplicationOperation;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.TRANSACTION_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.TRANSACTION_DS_FACTORY_ID;

public final class TransactionDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(TRANSACTION_DS_FACTORY, TRANSACTION_DS_FACTORY_ID);

    public static final int CREATE_TX_BACKUP_LOG = 0;
    public static final int BROADCAST_TX_ROLLBACK = 1;
    public static final int PURGE_TX_BACKUP_LOG = 2;
    public static final int REPLICATE_TX_BACKUP_LOG = 3;
    public static final int ROLLBACK_TX_BACKUP_LOG = 4;
    public static final int CREATE_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG = 5;
    public static final int PURGE_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG = 6;
    public static final int REPLICATE_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG = 7;
    public static final int ROLLBACK_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG = 8;
    public static final int CLEAR_REMOTE_TX_BACKUP = 9;
    public static final int CLEAR_REMOTE_TX = 10;
    public static final int COLLECT_REMOTE_TX = 11;
    public static final int COLLECT_REMOTE_TX_FACTORY = 12;
    public static final int FINALIZE_REMOTE_TX_BACKUP = 13;
    public static final int FINALIZE_REMOTE_TX = 14;
    public static final int PUT_REMOTE_TX_BACKUP = 15;
    public static final int PUT_REMOTE_TX = 16;
    public static final int XA_REPLICATION = 17;
    public static final int XA_TRANSACTION_DTO = 18;


    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case CREATE_TX_BACKUP_LOG:
                        return new CreateTxBackupLogOperation();
                    case BROADCAST_TX_ROLLBACK:
                        return new BroadcastTxRollbackOperation();
                    case PURGE_TX_BACKUP_LOG:
                        return new PurgeTxBackupLogOperation();
                    case REPLICATE_TX_BACKUP_LOG:
                        return new ReplicateTxBackupLogOperation();
                    case ROLLBACK_TX_BACKUP_LOG:
                        return new RollbackTxBackupLogOperation();
                    case CREATE_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG:
                        return new CreateAllowedDuringPassiveStateTxBackupLogOperation();
                    case PURGE_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG:
                        return new PurgeAllowedDuringPassiveStateTxBackupLogOperation();
                    case REPLICATE_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG:
                        return new ReplicateAllowedDuringPassiveStateTxBackupLogOperation();
                    case ROLLBACK_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG:
                        return new RollbackAllowedDuringPassiveStateTxBackupLogOperation();
                    case CLEAR_REMOTE_TX_BACKUP:
                        return new ClearRemoteTransactionBackupOperation();
                    case CLEAR_REMOTE_TX:
                        return new ClearRemoteTransactionOperation();
                    case COLLECT_REMOTE_TX:
                        return new CollectRemoteTransactionsOperation();
                    case FINALIZE_REMOTE_TX_BACKUP:
                        return new FinalizeRemoteTransactionBackupOperation();
                    case FINALIZE_REMOTE_TX:
                        return new FinalizeRemoteTransactionOperation();
                    case PUT_REMOTE_TX_BACKUP:
                        return new PutRemoteTransactionBackupOperation();
                    case PUT_REMOTE_TX:
                        return new PutRemoteTransactionOperation();
                    case XA_REPLICATION:
                        return new XaReplicationOperation();
                    case XA_TRANSACTION_DTO:
                        return new XATransactionDTO();
                    default:
                        return null;
                }
            }
        };
    }
}
