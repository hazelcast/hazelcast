/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.transaction.impl.operations.BeginTxBackupOperation;
import com.hazelcast.transaction.impl.operations.BroadcastTxRollbackOperation;
import com.hazelcast.transaction.impl.operations.PurgeTxBackupOperation;
import com.hazelcast.transaction.impl.operations.ReplicateTxOperation;
import com.hazelcast.transaction.impl.operations.RollbackTxBackupOperation;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.TRANSACTION_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.TRANSACTION_DS_FACTORY_ID;

public final class TransactionDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(TRANSACTION_DS_FACTORY, TRANSACTION_DS_FACTORY_ID);

    public static final int BEGIN_TX_BACKUP = 0;
    public static final int BROADCAST_TX_ROLLBACK = 1;
    public static final int PURGE_TX_BACKUP = 2;
    public static final int REPLICATE_TX = 3;
    public static final int ROLLBACK_TX_BACKUP = 4;

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
                    case BEGIN_TX_BACKUP:
                        return new BeginTxBackupOperation();
                    case BROADCAST_TX_ROLLBACK:
                        return new BroadcastTxRollbackOperation();
                    case PURGE_TX_BACKUP:
                        return new PurgeTxBackupOperation();
                    case REPLICATE_TX:
                        return new ReplicateTxOperation();
                    case ROLLBACK_TX_BACKUP:
                        return new RollbackTxBackupOperation();
                    default:
                        return null;
                }
            }
        };
    }
}
