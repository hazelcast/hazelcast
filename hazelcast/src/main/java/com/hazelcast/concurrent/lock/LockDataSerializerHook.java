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

package com.hazelcast.concurrent.lock;

import com.hazelcast.concurrent.lock.operations.AwaitOperation;
import com.hazelcast.concurrent.lock.operations.AwaitBackupOperation;
import com.hazelcast.concurrent.lock.operations.BeforeAwaitBackupOperation;
import com.hazelcast.concurrent.lock.operations.BeforeAwaitOperation;
import com.hazelcast.concurrent.lock.operations.GetLockCountOperation;
import com.hazelcast.concurrent.lock.operations.GetRemainingLeaseTimeOperation;
import com.hazelcast.concurrent.lock.operations.IsLockedOperation;
import com.hazelcast.concurrent.lock.operations.LockBackupOperation;
import com.hazelcast.concurrent.lock.operations.LockOperation;
import com.hazelcast.concurrent.lock.operations.LockReplicationOperation;
import com.hazelcast.concurrent.lock.operations.SignalBackupOperation;
import com.hazelcast.concurrent.lock.operations.SignalOperation;
import com.hazelcast.concurrent.lock.operations.UnlockBackupOperation;
import com.hazelcast.concurrent.lock.operations.UnlockOperation;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public final class LockDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.LOCK_DS_FACTORY, -15);

    public static final int AWAIT_BACKUP = 0;
    public static final int AWAIT = 1;
    public static final int BEFORE_AWAIT_BACKUP = 2;
    public static final int BEFORE_AWAIT = 3;
    public static final int GET_LOCK_COUNT = 4;
    public static final int GET_REMAINING_LEASETIME = 5;
    public static final int IS_LOCKED = 6;
    public static final int LOCK_BACKUP = 7;
    public static final int LOCK = 8;
    public static final int LOCK_REPLICATION = 9;
    public static final int SIGNAL_BACKUP = 10;
    public static final int SIGNAL = 11;
    public static final int UNLOCK_BACKUP = 12;
    public static final int UNLOCK = 13;


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
                    case AWAIT_BACKUP:
                        return new AwaitBackupOperation();
                    case AWAIT:
                        return new AwaitOperation();
                    case BEFORE_AWAIT_BACKUP:
                        return new BeforeAwaitBackupOperation();
                    case BEFORE_AWAIT:
                        return new BeforeAwaitOperation();
                    case GET_LOCK_COUNT:
                        return new GetLockCountOperation();
                    case GET_REMAINING_LEASETIME:
                        return new GetRemainingLeaseTimeOperation();
                    case IS_LOCKED:
                        return new IsLockedOperation();
                    case LOCK:
                        return new LockOperation();
                    case LOCK_BACKUP:
                        return new LockBackupOperation();
                    case LOCK_REPLICATION:
                        return new LockReplicationOperation();
                    case SIGNAL_BACKUP:
                        return new SignalBackupOperation();
                    case SIGNAL:
                        return new SignalOperation();
                    case UNLOCK_BACKUP:
                        return new UnlockBackupOperation();
                    case UNLOCK:
                        return new UnlockOperation();
                    default:
                        return null;
                }
            }
        };
    }
}
