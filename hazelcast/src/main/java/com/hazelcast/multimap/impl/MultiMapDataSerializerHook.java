/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.multimap.impl.operations.ClearBackupOperation;
import com.hazelcast.multimap.impl.operations.ClearOperation;
import com.hazelcast.multimap.impl.operations.ContainsEntryOperation;
import com.hazelcast.multimap.impl.operations.CountOperation;
import com.hazelcast.multimap.impl.operations.EntrySetOperation;
import com.hazelcast.multimap.impl.operations.GetAllOperation;
import com.hazelcast.multimap.impl.operations.KeySetOperation;
import com.hazelcast.multimap.impl.operations.PutBackupOperation;
import com.hazelcast.multimap.impl.operations.PutOperation;
import com.hazelcast.multimap.impl.operations.RemoveAllBackupOperation;
import com.hazelcast.multimap.impl.operations.RemoveAllOperation;
import com.hazelcast.multimap.impl.operations.RemoveBackupOperation;
import com.hazelcast.multimap.impl.operations.RemoveOperation;
import com.hazelcast.multimap.impl.operations.SizeOperation;
import com.hazelcast.multimap.impl.operations.ValuesOperation;
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
import com.hazelcast.nio.serialization.ArrayDataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;

public class MultiMapDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.MULTIMAP_DS_FACTORY, -12);

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


    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors
                = new ConstructorFunction[TXN_ROLLBACK_BACKUP + 1];
        constructors[CLEAR_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClearBackupOperation();
            }
        };
        constructors[CLEAR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClearOperation();
            }
        };
        constructors[CONTAINS_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ContainsEntryOperation();
            }
        };
        constructors[COUNT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CountOperation();
            }
        };
        constructors[ENTRY_SET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new EntrySetOperation();
            }
        };
        constructors[GET_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new GetAllOperation();
            }
        };
        constructors[KEY_SET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new KeySetOperation();
            }
        };
        constructors[PUT_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PutBackupOperation();
            }
        };
        constructors[PUT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PutOperation();
            }
        };
        constructors[REMOVE_ALL_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RemoveAllBackupOperation();
            }
        };
        constructors[REMOVE_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RemoveAllOperation();
            }
        };
        constructors[REMOVE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RemoveBackupOperation();
            }
        };
        constructors[REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RemoveOperation();
            }
        };
        constructors[SIZE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SizeOperation();
            }
        };
        constructors[VALUES] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ValuesOperation();
            }
        };


        constructors[TXN_COMMIT_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnCommitBackupOperation();
            }
        };
        constructors[TXN_COMMIT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnCommitOperation();
            }
        };
        constructors[TXN_GENERATE_RECORD_ID] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnGenerateRecordIdOperation();
            }
        };
        constructors[TXN_LOCK_AND_GET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnLockAndGetOperation();
            }
        };
        constructors[TXN_PREPARE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnPrepareBackupOperation();
            }
        };
        constructors[TXN_PREPARE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnPrepareOperation();
            }
        };
        constructors[TXN_PUT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnPutOperation();
            }
        };
        constructors[TXN_PUT_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnPutBackupOperation();
            }
        };
        constructors[TXN_REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnRemoveOperation();
            }
        };
        constructors[TXN_REMOVE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnRemoveBackupOperation();
            }
        };
        constructors[TXN_REMOVE_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnRemoveAllOperation();
            }
        };
        constructors[TXN_REMOVE_ALL_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnRemoveAllBackupOperation();
            }
        };
        constructors[TXN_ROLLBACK_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnRollbackBackupOperation();
            }
        };
        constructors[TXN_ROLLBACK] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnRollbackOperation();
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }
}
