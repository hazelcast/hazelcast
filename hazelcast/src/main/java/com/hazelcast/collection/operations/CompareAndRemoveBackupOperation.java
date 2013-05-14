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
import com.hazelcast.collection.CollectionWrapper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @ali 1/21/13
 */
public class CompareAndRemoveBackupOperation extends CollectionKeyBasedOperation implements BackupOperation {

    Set<Long> idSet;

    public CompareAndRemoveBackupOperation() {
    }

    public CompareAndRemoveBackupOperation(CollectionProxyId proxyId, Data dataKey, Set<Long> idSet) {
        super(proxyId, dataKey);
        this.idSet = idSet;
    }

    public void run() throws Exception {
        CollectionWrapper wrapper = getCollectionWrapper();
        if (wrapper == null){
            response = false;
            return;
        }
        Iterator<CollectionRecord> iter = wrapper.getCollection().iterator();
        while (iter.hasNext()) {
            CollectionRecord record = iter.next();
            if (idSet.contains(record.getRecordId())) {
                iter.remove();
            }
        }
        response = true;
    }

    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(idSet.size());
        for (Long id : idSet) {
            out.writeLong(id);
        }
    }

    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        idSet = new HashSet<Long>(size);
        for (int i = 0; i < size; i++) {
            idSet.add(in.readLong());
        }
    }

    public int getId() {
        return CollectionDataSerializerHook.COMPARE_AND_REMOVE_BACKUP;
    }
}
