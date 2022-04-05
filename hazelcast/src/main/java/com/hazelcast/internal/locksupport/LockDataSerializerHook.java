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

package com.hazelcast.internal.locksupport;

import com.hazelcast.internal.locksupport.operations.IsLockedOperation;
import com.hazelcast.internal.locksupport.operations.LockBackupOperation;
import com.hazelcast.internal.locksupport.operations.LockOperation;
import com.hazelcast.internal.locksupport.operations.LockReplicationOperation;
import com.hazelcast.internal.locksupport.operations.UnlockBackupOperation;
import com.hazelcast.internal.locksupport.operations.UnlockIfLeaseExpiredOperation;
import com.hazelcast.internal.locksupport.operations.UnlockOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.LOCK_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.LOCK_DS_FACTORY_ID;

public final class LockDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(LOCK_DS_FACTORY, LOCK_DS_FACTORY_ID);

    public static final int LOCK_RESOURCE = 0;
    public static final int LOCK_STORE = 1;
    public static final int IS_LOCKED = 9;
    public static final int LOCK_BACKUP = 10;
    public static final int LOCK = 11;
    public static final int LOCK_REPLICATION = 12;
    public static final int UNLOCK_BACKUP = 15;
    public static final int UNLOCK = 16;
    public static final int UNLOCK_IF_LEASE_EXPIRED = 17;


    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case IS_LOCKED:
                    return new IsLockedOperation();
                case LOCK:
                    return new LockOperation();
                case LOCK_BACKUP:
                    return new LockBackupOperation();
                case LOCK_REPLICATION:
                    return new LockReplicationOperation();
                case UNLOCK_BACKUP:
                    return new UnlockBackupOperation();
                case UNLOCK:
                    return new UnlockOperation();
                case LOCK_STORE:
                    return new LockStoreImpl();
                case LOCK_RESOURCE:
                    return new LockResourceImpl();
                case UNLOCK_IF_LEASE_EXPIRED:
                    return new UnlockIfLeaseExpiredOperation();
                default:
                    return null;
            }
        };
    }
}
