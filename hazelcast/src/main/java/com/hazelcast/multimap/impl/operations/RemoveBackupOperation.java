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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import static com.hazelcast.internal.cluster.Versions.V3_12;

public class RemoveBackupOperation extends AbstractKeyBasedMultiMapOperation implements BackupOperation, Versioned {

    private long recordId;
    private Data value;

    public RemoveBackupOperation() {
    }

    public RemoveBackupOperation(String name, Data dataKey, long recordId, Data value) {
        super(name, dataKey);
        this.recordId = recordId;
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

        // RU_COMPAT_3_11
        if (value != null) {
            MultiMapRecord record = new MultiMapRecord(isBinary() ? value : toObject(value));
            response = coll.remove(record);
        } else {
            Iterator<MultiMapRecord> iterator = coll.iterator();
            while (iterator.hasNext()) {
                if (iterator.next().getRecordId() == recordId) {
                    iterator.remove();
                    response = true;
                    break;
                }
            }
        }

        if (coll.isEmpty()) {
            container.delete(dataKey);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(recordId);
        // RU_COMPAT_3_11
        if (out.getVersion().isGreaterOrEqual(V3_12)) {
            out.writeData(value);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        recordId = in.readLong();
        // RU_COMPAT_3_11
        if (in.getVersion().isGreaterOrEqual(V3_12)) {
            value = in.readData();
        }
    }

    @Override
    public int getId() {
        return MultiMapDataSerializerHook.REMOVE_BACKUP;
    }
}
