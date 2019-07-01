/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.tx;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.operation.PutBackupOperation;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.nio.serialization.Data;

public class TxnSetBackupOperation extends PutBackupOperation {

    public TxnSetBackupOperation() {
    }

    public TxnSetBackupOperation(String name, Data dataKey, Data dataValue,
                                 RecordInfo recordInfo, boolean putTransient,
                                 boolean disableWanReplicationEvent) {
        super(name, dataKey, dataValue, recordInfo, putTransient, disableWanReplicationEvent);
    }

    @Override
    protected void runInternal() {
        super.runInternal();

        recordStore.forceUnlock(dataKey);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.TXN_SET_BACKUP;
    }
}
