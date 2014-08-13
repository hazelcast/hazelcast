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
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapWrapper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

public class RemoveBackupOperation extends MultiMapKeyBasedOperation implements BackupOperation {

    long recordId;

    public RemoveBackupOperation() {
    }

    public RemoveBackupOperation(String name, Data dataKey, long recordId) {
        super(name, dataKey);
        this.recordId = recordId;
    }

    public void run() throws Exception {
        MultiMapWrapper wrapper = getCollectionWrapper();
        response = false;
        if (wrapper == null) {
            return;
        }
        Collection<MultiMapRecord> coll = wrapper.getCollection(false);
        Iterator<MultiMapRecord> iter = coll.iterator();
        while (iter.hasNext()) {
            if (iter.next().getRecordId() == recordId) {
                iter.remove();
                response = true;
                if (coll.isEmpty()) {
                    delete();
                }
                break;
            }
        }
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(recordId);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        recordId = in.readLong();
    }

    public int getId() {
        return MultiMapDataSerializerHook.REMOVE_BACKUP;
    }

}
