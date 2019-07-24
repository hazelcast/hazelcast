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
import com.hazelcast.map.impl.operation.RemoveBackupOperation;
import com.hazelcast.nio.serialization.Data;

public class TxnDeleteBackupOperation extends RemoveBackupOperation {

    public TxnDeleteBackupOperation() {
    }

    public TxnDeleteBackupOperation(String name, Data dataKey, boolean disableWanReplicationEvent) {
        super(name, dataKey, disableWanReplicationEvent);
    }

    @Override
    protected void runInternal() {
        super.runInternal();

        recordStore.forceUnlock(dataKey);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.TXN_DELETE_BACKUP;
    }
}
