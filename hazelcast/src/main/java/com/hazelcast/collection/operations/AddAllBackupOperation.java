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

package com.hazelcast.collection.operations;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @ali 1/17/13
 */
public class AddAllBackupOperation extends CollectionKeyBasedOperation implements BackupOperation {

    Collection<CollectionRecord> recordList;

    int index;

    public AddAllBackupOperation() {
    }

    public AddAllBackupOperation(CollectionProxyId proxyId, Data dataKey, Collection<CollectionRecord> recordList, int index) {
        super(proxyId, dataKey);
        this.recordList = recordList;
        this.index = index;
    }

    public void run() throws Exception {
        Collection<CollectionRecord> coll = getOrCreateCollectionWrapper().getCollection();
        if (index == -1) {
            response = coll.addAll(recordList);
        } else {
            List list = (List) coll;
            try {
                response = list.addAll(index, recordList);
            } catch (IndexOutOfBoundsException e) {
                response = e;
            }
        }
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(index);
        out.writeInt(recordList.size());
        for (CollectionRecord record : recordList) {
            record.writeData(out);
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        index = in.readInt();
        int size = in.readInt();
        recordList = new ArrayList<CollectionRecord>(size);
        for (int i = 0; i < size; i++) {
            CollectionRecord rec = new CollectionRecord();
            rec.readData(in);
            recordList.add(rec);
        }
    }

    public int getId() {
        return CollectionDataSerializerHook.ADD_ALL_BACKUP;
    }
}
