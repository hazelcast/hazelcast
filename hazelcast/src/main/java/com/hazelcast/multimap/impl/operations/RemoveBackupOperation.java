/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

public class RemoveBackupOperation extends AbstractKeyBasedMultiMapOperation implements BackupOperation {

    long recordId;

    public RemoveBackupOperation() {
    }

    public RemoveBackupOperation(String name, Data dataKey, long recordId) {
        super(name, dataKey);
        this.recordId = recordId;
    }

    @Override
    public void run() throws Exception {
        MultiMapValue multiMapValue = getMultiMapValueOrNull();
        response = false;
        if (multiMapValue == null) {
            return;
        }
        Collection<MultiMapRecord> coll = multiMapValue.getCollection(false);
        Iterator<MultiMapRecord> iterator = coll.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getRecordId() == recordId) {
                iterator.remove();
                response = true;
                if (coll.isEmpty()) {
                    delete();
                }
                break;
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(recordId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        recordId = in.readLong();
    }

    @Override
    public int getId() {
        return MultiMapDataSerializerHook.REMOVE_BACKUP;
    }
}
