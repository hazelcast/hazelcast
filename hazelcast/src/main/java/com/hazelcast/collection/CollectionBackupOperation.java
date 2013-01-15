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

package com.hazelcast.collection;

import com.hazelcast.collection.processor.BackupAwareEntryProcessor;
import com.hazelcast.collection.processor.Entry;
import com.hazelcast.collection.processor.EntryProcessor;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.AbstractNamedKeyBasedOperation;

import java.io.IOException;

/**
 * @ali 1/2/13
 */
public class CollectionBackupOperation extends AbstractNamedKeyBasedOperation implements BackupOperation, IdentifiedDataSerializable {

    EntryProcessor processor;

    CollectionProxyId proxyId;

    Address firstCaller;

    int firstThreadId;

    CollectionBackupOperation() {
    }

    CollectionBackupOperation(String name, Data dataKey, EntryProcessor processor, Address firstCaller, int firstThreadId, CollectionProxyId proxyId) {
        super(name, dataKey);
        this.processor = processor;
        this.firstCaller = firstCaller;
        this.firstThreadId = firstThreadId;
        this.proxyId = proxyId;
    }

    public void run() throws Exception {
        CollectionService service = getService();
        CollectionContainer collectionContainer = service.getOrCreateCollectionContainer(getPartitionId(), proxyId);
        ((BackupAwareEntryProcessor) processor).executeBackup(new Entry(collectionContainer, dataKey, firstThreadId, firstCaller));
    }

    public Object getResponse() {
        return true;
    }

    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(processor);
        out.writeInt(firstThreadId);
        firstCaller.writeData(out);
        proxyId.writeData(out);
    }

    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        processor = in.readObject();
        firstThreadId = in.readInt();
        firstCaller = new Address();
        firstCaller.readData(in);
        proxyId = new CollectionProxyId();
        proxyId.readData(in);
    }

    public int getId() {
        return DataSerializerCollectionHook.COLLECTION_BACKUP_OPERATION;
    }
}
