/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.AbstractNamedKeyBasedOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ali 1/2/13
 */
public class CollectionBackupOperation extends AbstractNamedKeyBasedOperation implements BackupOperation {

    EntryProcessor processor;

    CollectionBackupOperation() {
    }

    CollectionBackupOperation(String name, Data dataKey, EntryProcessor processor) {
        super(name, dataKey);
        this.processor = processor;
    }

    public void run() throws Exception {
        CollectionService service = getService();
        CollectionContainer collectionContainer = service.getCollectionContainer(getPartitionId(), name);
        ((BackupAwareEntryProcessor)processor).executeBackup(new Entry(collectionContainer, dataKey));
    }

    public Object getResponse() {
        return true;
    }

    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeObject(out, processor);
    }

    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        processor = IOUtil.readObject(in);
    }
}
