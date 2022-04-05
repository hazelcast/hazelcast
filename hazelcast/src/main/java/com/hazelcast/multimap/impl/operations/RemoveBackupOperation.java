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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;
import java.util.Collection;

public class RemoveBackupOperation extends AbstractKeyBasedMultiMapOperation implements BackupOperation {

    private Data value;

    public RemoveBackupOperation() {
    }

    public RemoveBackupOperation(String name, Data dataKey, Data value) {
        super(name, dataKey);
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        response = false;
        MultiMapContainer container = getOrCreateContainerWithoutAccess();
        MultiMapValue multiMapValue = container.getMultiMapValueOrNull(dataKey);
        if (multiMapValue == null) {
            return;
        }
        Collection<MultiMapRecord> coll = multiMapValue.getCollection(false);

        MultiMapRecord record = new MultiMapRecord(isBinary() ? value : toObject(value));
        response = coll.remove(record);

        if (coll.isEmpty()) {
            container.delete(dataKey);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeData(out, value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = IOUtil.readData(in);
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.REMOVE_BACKUP;
    }
}
