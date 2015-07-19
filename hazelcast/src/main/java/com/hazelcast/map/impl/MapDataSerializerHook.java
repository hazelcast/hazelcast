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

package com.hazelcast.map.impl;

import com.hazelcast.map.impl.operation.GetOperation;
import com.hazelcast.map.impl.operation.PutBackupOperation;
import com.hazelcast.map.impl.operation.PutOperation;
import com.hazelcast.map.impl.operation.RemoveBackupOperation;
import com.hazelcast.map.impl.operation.RemoveOperation;
import com.hazelcast.map.impl.tx.MapTransactionLogRecord;
import com.hazelcast.map.impl.tx.operations.TxnDeleteOperation;
import com.hazelcast.map.impl.tx.operations.TxnLockAndGetOperation;
import com.hazelcast.map.impl.tx.operations.TxnPrepareBackupOperation;
import com.hazelcast.map.impl.tx.operations.TxnPrepareOperation;
import com.hazelcast.map.impl.tx.operations.TxnRollbackBackupOperation;
import com.hazelcast.map.impl.tx.operations.TxnRollbackOperation;
import com.hazelcast.map.impl.tx.operations.TxnSetOperation;
import com.hazelcast.map.impl.tx.operations.TxnUnlockBackupOperation;
import com.hazelcast.map.impl.tx.operations.TxnUnlockOperation;
import com.hazelcast.map.impl.tx.VersionedValue;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.FactoryIdHelper;
import com.hazelcast.query.impl.QueryResultEntryImpl;
import com.hazelcast.util.QueryResultSet;

import static com.hazelcast.nio.serialization.impl.FactoryIdHelper.MAP_DS_FACTORY;
import static com.hazelcast.nio.serialization.impl.FactoryIdHelper.MAP_DS_FACTORY_ID;

public final class MapDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(MAP_DS_FACTORY, MAP_DS_FACTORY_ID);

    public static final int PUT = 0;
    public static final int GET = 1;
    public static final int REMOVE = 2;
    public static final int PUT_BACKUP = 3;
    public static final int REMOVE_BACKUP = 4;
    public static final int KEY_SET = 8;
    public static final int VALUES = 9;
    public static final int ENTRY_SET = 10;
    public static final int ENTRY_VIEW = 11;
    public static final int QUERY_RESULT_ENTRY = 13;
    public static final int QUERY_RESULT_SET = 14;

    public static final int TXN_LOG_RECORD = 15;
    public static final int TXN_DELETE = 16;
    public static final int TXN_LOCK_AND_GET = 17;
    public static final int TXN_PREPARE_BACKUP = 18;
    public static final int TXN_PREPARE = 19;
    public static final int TXN_ROLLBACK_BACKUP = 20;
    public static final int TXN_ROLLBACK = 21;
    public static final int TXN_SET = 22;
    public static final int TXN_UNLOCK_BACKUP = 23;
    public static final int TXN_UNLOCK = 24;
    public static final int VERSIONED_VALUE = 25;

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
                    case PUT:
                        return new PutOperation();
                    case GET:
                        return new GetOperation();
                    case REMOVE:
                        return new RemoveOperation();
                    case PUT_BACKUP:
                        return new PutBackupOperation();
                    case REMOVE_BACKUP:
                        return new RemoveBackupOperation();
                    case KEY_SET:
                        return new MapKeySet();
                    case VALUES:
                        return new MapValueCollection();
                    case ENTRY_SET:
                        return new MapEntrySet();
                    case ENTRY_VIEW:
                        return (IdentifiedDataSerializable) EntryViews.createSimpleEntryView();
                    case QUERY_RESULT_ENTRY:
                        return new QueryResultEntryImpl();
                    case QUERY_RESULT_SET:
                        return new QueryResultSet();
                    case TXN_LOG_RECORD:
                        return new MapTransactionLogRecord();
                    case TXN_DELETE:
                        return new TxnDeleteOperation();
                    case TXN_LOCK_AND_GET:
                        return new TxnLockAndGetOperation();
                    case TXN_PREPARE_BACKUP:
                        return new TxnPrepareBackupOperation();
                    case TXN_PREPARE:
                        return new TxnPrepareOperation();
                    case TXN_ROLLBACK_BACKUP:
                        return new TxnRollbackBackupOperation();
                    case TXN_ROLLBACK:
                        return new TxnRollbackOperation();
                    case TXN_SET:
                        return new TxnSetOperation();
                    case TXN_UNLOCK_BACKUP:
                        return new TxnUnlockBackupOperation();
                    case TXN_UNLOCK:
                        return new TxnUnlockOperation();
                    case VERSIONED_VALUE:
                        return new VersionedValue();
                    default:
                        return null;
                }
            }
        };
    }
}
