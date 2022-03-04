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

package com.hazelcast.multimap.impl.txn;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.multimap.impl.operations.AbstractKeyBasedMultiMapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

public class TxnRemoveBackupOperation extends AbstractKeyBasedMultiMapOperation implements BackupOperation {

    private long recordId;
    private Data value;

    public TxnRemoveBackupOperation() {
    }

    public TxnRemoveBackupOperation(String name, Data dataKey, long recordId, Data value) {
        super(name, dataKey);
        this.recordId = recordId;
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainerWithoutAccess();
        MultiMapValue multiMapValue = container.getMultiMapValueOrNull(dataKey);
        if (multiMapValue == null || !multiMapValue.containsRecordId(recordId)) {
            response = false;
            return;
        }
        response = true;
        Collection<MultiMapRecord> coll = multiMapValue.getCollection(false);
        Iterator<MultiMapRecord> iterator = coll.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getRecordId() == recordId) {
                iterator.remove();
                break;
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
        IOUtil.writeData(out, value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        recordId = in.readLong();
        value = IOUtil.readData(in);
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.TXN_REMOVE_BACKUP;
    }
}
