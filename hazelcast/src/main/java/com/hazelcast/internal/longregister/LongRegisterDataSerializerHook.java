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

package com.hazelcast.internal.longregister;

import com.hazelcast.internal.longregister.operations.AddAndGetOperation;
import com.hazelcast.internal.longregister.operations.AddBackupOperation;
import com.hazelcast.internal.longregister.operations.GetAndAddOperation;
import com.hazelcast.internal.longregister.operations.GetAndSetOperation;
import com.hazelcast.internal.longregister.operations.GetOperation;
import com.hazelcast.internal.longregister.operations.LongRegisterReplicationOperation;
import com.hazelcast.internal.longregister.operations.SetBackupOperation;
import com.hazelcast.internal.longregister.operations.SetOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.DataSerializableFactory;


public final class LongRegisterDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = -999999999;

    public static final int ADD_BACKUP = 0;
    public static final int ADD_AND_GET = 1;
    public static final int GET = 2;
    public static final int GET_AND_SET = 3;
    public static final int GET_AND_ADD = 4;
    public static final int SET_OPERATION = 5;
    public static final int SET_BACKUP = 6;
    public static final int REPLICATION = 7;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case ADD_BACKUP:
                    return new AddBackupOperation();
                case ADD_AND_GET:
                    return new AddAndGetOperation();
                case GET:
                    return new GetOperation();
                case GET_AND_SET:
                    return new GetAndSetOperation();
                case GET_AND_ADD:
                    return new GetAndAddOperation();
                case SET_OPERATION:
                    return new SetOperation();
                case SET_BACKUP:
                    return new SetBackupOperation();
                case REPLICATION:
                    return new LongRegisterReplicationOperation();
                default:
                    throw new IllegalArgumentException();
            }
        };
    }
}
