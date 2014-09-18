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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

public class RemoveAllBackupOperation extends MultiMapKeyBasedOperation implements BackupOperation {

    public RemoveAllBackupOperation() {
    }

    public RemoveAllBackupOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public void run() throws Exception {
        delete();
        response = true;
    }

    public int getId() {
        return MultiMapDataSerializerHook.REMOVE_ALL_BACKUP;
    }
}
