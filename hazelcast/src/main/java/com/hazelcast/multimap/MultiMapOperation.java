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

package com.hazelcast.multimap;

import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.multimap.processor.BackupAwareEntryProcessor;
import com.hazelcast.multimap.processor.Entry;
import com.hazelcast.multimap.processor.EntryProcessor;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.AbstractNamedKeyBasedOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ali 1/1/13
 */
public class MultiMapOperation extends AbstractNamedKeyBasedOperation implements BackupAwareOperation {

    EntryProcessor processor;

    transient Object response;

    public MultiMapOperation() {
    }

    public MultiMapOperation(String name, Data dataKey, EntryProcessor processor, int partitionId) {
        super(name, dataKey);
        this.processor = processor;
        setPartitionId(partitionId);
    }

    public void run() throws Exception {
        MultiMapService mmService = getService();
        MultiMapContainer multiMap = mmService.getMultiMap(getPartitionId(), name);
        Object entryValue = multiMap.getObject(dataKey);
        if (entryValue == null){
            entryValue = processor.createNew();
            multiMap.putObject(dataKey, entryValue);
        }
        response = processor.execute(new Entry(dataKey, entryValue));
    }

    public Object getResponse() {
        return response;
    }

    public boolean shouldBackup() {
        return processor instanceof BackupAwareEntryProcessor && response != null;
    }

    public int getSyncBackupCount() {
        MultiMapService mmService = getService();
        MultiMapConfig config = mmService.getConfig(name);
        //TODO config
        return 1;
    }

    public int getAsyncBackupCount() {
        MultiMapService mmService = getService();
        MultiMapConfig config = mmService.getConfig(name);
        //TODO config
        return 0;
    }

    public Operation getBackupOperation() {
        return new MultiMapBackupOperation(name, dataKey, (BackupAwareEntryProcessor)processor);
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
