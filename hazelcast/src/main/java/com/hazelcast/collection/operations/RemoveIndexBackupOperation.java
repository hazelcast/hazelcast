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
import java.util.List;

/**
 * @ali 1/17/13
 */
public class RemoveIndexBackupOperation extends CollectionKeyBasedOperation implements BackupOperation {

    int index;

    public RemoveIndexBackupOperation() {
    }

    public RemoveIndexBackupOperation(CollectionProxyId proxyId, Data dataKey, int index) {
        super(proxyId, dataKey);
        this.index = index;
    }

    public void run() throws Exception {
        List<CollectionRecord> list = (List<CollectionRecord>) getOrCreateCollectionWrapper().getCollection();
        list.remove(index);
        response = true;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(index);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        index = in.readInt();
    }

    public int getId() {
        return CollectionDataSerializerHook.REMOVE_INDEX_BACKUP;
    }
}
