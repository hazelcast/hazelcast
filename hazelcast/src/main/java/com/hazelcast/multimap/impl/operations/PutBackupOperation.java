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

import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import java.io.IOException;

public class PutBackupOperation extends AbstractKeyBasedMultiMapOperation implements BackupOperation {

    private long recordId;
    private Data value;
    private int index;

    public PutBackupOperation() {
    }

    public PutBackupOperation(String name, Data dataKey, Data value, long recordId, int index) {
        super(name, dataKey);
        this.value = value;
        this.recordId = recordId;
        this.index = index;
    }

    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainerWithoutAccess();
        MultiMapRecord record = new MultiMapRecord(recordId, isBinary() ? value : toObject(value));
        try {
            response = container.addValue(dataKey, record, index);
        } catch (IndexOutOfBoundsException e) {
            response = e;
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(recordId);
        out.writeInt(index);
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        recordId = in.readLong();
        index = in.readInt();
        value = in.readData();
    }

    @Override
    public int getId() {
        return MultiMapDataSerializerHook.PUT_BACKUP;
    }
}
