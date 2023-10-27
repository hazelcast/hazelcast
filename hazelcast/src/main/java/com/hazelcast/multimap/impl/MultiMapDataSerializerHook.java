/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.multimap.impl.operations.ClearBackupOperation;
import com.hazelcast.multimap.impl.operations.ClearOperation;
import com.hazelcast.multimap.impl.operations.ContainsEntryOperation;
import com.hazelcast.multimap.impl.operations.CountOperation;
import com.hazelcast.multimap.impl.operations.DeleteBackupOperation;
import com.hazelcast.multimap.impl.operations.DeleteOperation;
import com.hazelcast.multimap.impl.operations.EntrySetOperation;
import com.hazelcast.multimap.impl.operations.EntrySetResponse;
import com.hazelcast.multimap.impl.operations.GetAllOperation;
import com.hazelcast.multimap.impl.operations.KeySetOperation;
import com.hazelcast.multimap.impl.operations.MergeBackupOperation;
import com.hazelcast.multimap.impl.operations.MergeOperation;
import com.hazelcast.multimap.impl.operations.MultiMapOperationFactory;
import com.hazelcast.multimap.impl.operations.MultiMapPutAllOperationFactory;
import com.hazelcast.multimap.impl.operations.MultiMapReplicationOperation;
import com.hazelcast.multimap.impl.operations.MultiMapResponse;
import com.hazelcast.multimap.impl.operations.PutAllBackupOperation;
import com.hazelcast.multimap.impl.operations.PutAllOperation;
import com.hazelcast.multimap.impl.operations.PutBackupOperation;
import com.hazelcast.multimap.impl.operations.PutOperation;
import com.hazelcast.multimap.impl.operations.RemoveAllBackupOperation;
import com.hazelcast.multimap.impl.operations.RemoveAllOperation;
import com.hazelcast.multimap.impl.operations.RemoveBackupOperation;
import com.hazelcast.multimap.impl.operations.RemoveOperation;
import com.hazelcast.multimap.impl.operations.SizeOperation;
import com.hazelcast.multimap.impl.operations.ValuesOperation;
import com.hazelcast.multimap.impl.txn.MultiMapTransactionLogRecord;
import com.hazelcast.multimap.impl.txn.TxnCommitBackupOperation;
import com.hazelcast.multimap.impl.txn.TxnCommitOperation;
import com.hazelcast.multimap.impl.txn.TxnGenerateRecordIdOperation;
import com.hazelcast.multimap.impl.txn.TxnLockAndGetOperation;
import com.hazelcast.multimap.impl.txn.TxnPrepareBackupOperation;
import com.hazelcast.multimap.impl.txn.TxnPrepareOperation;
import com.hazelcast.multimap.impl.txn.TxnPutBackupOperation;
import com.hazelcast.multimap.impl.txn.TxnPutOperation;
import com.hazelcast.multimap.impl.txn.TxnRemoveAllBackupOperation;
import com.hazelcast.multimap.impl.txn.TxnRemoveAllOperation;
import com.hazelcast.multimap.impl.txn.TxnRemoveBackupOperation;
import com.hazelcast.multimap.impl.txn.TxnRemoveOperation;
import com.hazelcast.multimap.impl.txn.TxnRollbackBackupOperation;
import com.hazelcast.multimap.impl.txn.TxnRollbackOperation;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.function.Supplier;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.MULTIMAP_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.MULTIMAP_DS_FACTORY_ID;

public class MultiMapDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(MULTIMAP_DS_FACTORY, MULTIMAP_DS_FACTORY_ID);

    public static final int ADD_ALL_BACKUP = 0;
    public static final int ADD_ALL = 1;
    public static final int CLEAR_BACKUP = 2;
    public static final int CLEAR = 3;
    public static final int COMPARE_AND_REMOVE_BACKUP = 4;
    public static final int COMPARE_AND_REMOVE = 5;
    public static final int CONTAINS_ALL = 6;
    public static final int CONTAINS_ENTRY = 7;
    public static final int CONTAINS = 8;
    public static final int COUNT = 9;
    public static final int ENTRY_SET = 10;
    public static final int GET_ALL = 11;
    public static final int GET = 12;
    public static final int INDEX_OF = 13;
    public static final int KEY_SET = 14;
    public static final int PUT_BACKUP = 15;
    public static final int PUT = 16;
    public static final int REMOVE_ALL_BACKUP = 17;
    public static final int REMOVE_ALL = 18;
    public static final int REMOVE_BACKUP = 19;
    public static final int REMOVE = 20;
    public static final int REMOVE_INDEX_BACKUP = 21;
    public static final int REMOVE_INDEX = 22;
    public static final int SET_BACKUP = 23;
    public static final int SET = 24;
    public static final int SIZE = 25;
    public static final int VALUES = 26;

    public static final int TXN_COMMIT_BACKUP = 27;
    public static final int TXN_COMMIT = 28;
    public static final int TXN_GENERATE_RECORD_ID = 29;
    public static final int TXN_LOCK_AND_GET = 30;
    public static final int TXN_PREPARE_BACKUP = 31;
    public static final int TXN_PREPARE = 32;
    public static final int TXN_PUT = 33;
    public static final int TXN_PUT_BACKUP = 34;
    public static final int TXN_REMOVE = 35;
    public static final int TXN_REMOVE_BACKUP = 36;
    public static final int TXN_REMOVE_ALL = 37;
    public static final int TXN_REMOVE_ALL_BACKUP = 38;
    public static final int TXN_ROLLBACK = 39;
    public static final int TXN_ROLLBACK_BACKUP = 40;
    public static final int MULTIMAP_OP_FACTORY = 41;
    public static final int MULTIMAP_TRANSACTION_LOG_RECORD = 42;
    public static final int MULTIMAP_EVENT_FILTER = 43;
    public static final int MULTIMAP_RECORD = 44;
    public static final int MULTIMAP_REPLICATION_OPERATION = 45;
    public static final int MULTIMAP_RESPONSE = 46;
    public static final int ENTRY_SET_RESPONSE = 47;
    public static final int MERGE_CONTAINER = 48;
    public static final int MERGE_OPERATION = 49;
    public static final int MERGE_BACKUP_OPERATION = 50;
    public static final int DELETE = 51;
    public static final int DELETE_BACKUP = 52;
    public static final int PUT_ALL = 53;
    public static final int PUT_ALL_BACKUP = 54;
    public static final int PUT_ALL_PARTITION_AWARE_FACTORY = 55;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[PUT_ALL_PARTITION_AWARE_FACTORY + 1];

        constructors[CLEAR_BACKUP] = ClearBackupOperation::new;
        constructors[CLEAR] = ClearOperation::new;
        constructors[CONTAINS_ENTRY] = ContainsEntryOperation::new;
        constructors[COUNT] = CountOperation::new;
        constructors[ENTRY_SET] = EntrySetOperation::new;
        constructors[GET_ALL] = GetAllOperation::new;
        constructors[KEY_SET] = KeySetOperation::new;
        constructors[PUT_BACKUP] = PutBackupOperation::new;
        constructors[PUT] = PutOperation::new;
        constructors[REMOVE_ALL_BACKUP] = RemoveAllBackupOperation::new;
        constructors[REMOVE_ALL] = RemoveAllOperation::new;
        constructors[REMOVE_BACKUP] = RemoveBackupOperation::new;
        constructors[REMOVE] = RemoveOperation::new;
        constructors[SIZE] = SizeOperation::new;
        constructors[VALUES] = ValuesOperation::new;
        constructors[TXN_COMMIT_BACKUP] = TxnCommitBackupOperation::new;
        constructors[TXN_COMMIT] = TxnCommitOperation::new;
        constructors[TXN_GENERATE_RECORD_ID] = TxnGenerateRecordIdOperation::new;
        constructors[TXN_LOCK_AND_GET] = TxnLockAndGetOperation::new;
        constructors[TXN_PREPARE_BACKUP] = TxnPrepareBackupOperation::new;
        constructors[TXN_PREPARE] = TxnPrepareOperation::new;
        constructors[TXN_PUT] = TxnPutOperation::new;
        constructors[TXN_PUT_BACKUP] = TxnPutBackupOperation::new;
        constructors[TXN_REMOVE] = TxnRemoveOperation::new;
        constructors[TXN_REMOVE_BACKUP] = TxnRemoveBackupOperation::new;
        constructors[TXN_REMOVE_ALL] = TxnRemoveAllOperation::new;
        constructors[TXN_REMOVE_ALL_BACKUP] = TxnRemoveAllBackupOperation::new;
        constructors[TXN_ROLLBACK_BACKUP] = TxnRollbackBackupOperation::new;
        constructors[TXN_ROLLBACK] = TxnRollbackOperation::new;
        constructors[MULTIMAP_OP_FACTORY] = MultiMapOperationFactory::new;
        constructors[MULTIMAP_TRANSACTION_LOG_RECORD] = MultiMapTransactionLogRecord::new;
        constructors[MULTIMAP_EVENT_FILTER] = MultiMapEventFilter::new;
        constructors[MULTIMAP_RECORD] = MultiMapRecord::new;
        constructors[MULTIMAP_REPLICATION_OPERATION] = MultiMapReplicationOperation::new;
        constructors[MULTIMAP_RESPONSE] = MultiMapResponse::new;
        constructors[ENTRY_SET_RESPONSE] = EntrySetResponse::new;
        constructors[MERGE_CONTAINER] = MultiMapMergeContainer::new;
        constructors[MERGE_OPERATION] = MergeOperation::new;
        constructors[MERGE_BACKUP_OPERATION] = MergeBackupOperation::new;
        constructors[DELETE] = DeleteOperation::new;
        constructors[DELETE_BACKUP] = DeleteBackupOperation::new;
        constructors[PUT_ALL] = PutAllOperation::new;
        constructors[PUT_ALL_BACKUP] = PutAllBackupOperation::new;
        constructors[PUT_ALL_PARTITION_AWARE_FACTORY] = MultiMapPutAllOperationFactory::new;

        return new ArrayDataSerializableFactory(constructors);
    }
}
