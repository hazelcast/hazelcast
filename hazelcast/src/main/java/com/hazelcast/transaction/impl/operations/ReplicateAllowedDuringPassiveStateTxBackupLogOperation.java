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

package com.hazelcast.transaction.impl.operations;

import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.transaction.impl.TransactionLogRecord;

import java.util.List;

import static com.hazelcast.transaction.impl.TransactionDataSerializerHook.REPLICATE_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG;

public final class ReplicateAllowedDuringPassiveStateTxBackupLogOperation
        extends ReplicateTxBackupLogOperation
        implements AllowedDuringPassiveState {

    public ReplicateAllowedDuringPassiveStateTxBackupLogOperation() {
    }

    public ReplicateAllowedDuringPassiveStateTxBackupLogOperation(List<TransactionLogRecord> logs, String callerUuid,
                                                                  String txnId, long timeoutMillis, long startTime) {
        super(logs, callerUuid, txnId, timeoutMillis, startTime);
    }

    @Override
    public int getId() {
        return REPLICATE_ALLOWED_DURING_PASSIVE_STATE_TX_BACKUP_LOG;
    }

}
