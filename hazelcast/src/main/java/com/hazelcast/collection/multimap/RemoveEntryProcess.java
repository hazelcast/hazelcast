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

package com.hazelcast.collection.multimap;

import com.hazelcast.collection.processor.BackupAwareEntryProcessor;
import com.hazelcast.collection.processor.Entry;
import com.hazelcast.collection.processor.WaitSupportedEntryProcessor;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.Collection;

/**
 * @ali 1/3/13
 */
public class RemoveEntryProcess extends MultiMapEntryProcessor<Boolean> implements BackupAwareEntryProcessor, WaitSupportedEntryProcessor {

    Data data;

    public RemoveEntryProcess() {
    }

    public RemoveEntryProcess(Data data, MultiMapConfig config) {
        super(config.isBinary());
        this.data = data;
        this.syncBackupCount = config.getSyncBackupCount();
        this.asyncBackupCount = config.getAsyncBackupCount();
    }

    public Boolean execute(Entry entry) {
        Collection coll = entry.getValue();
        if (coll == null){
            return false;
        }
        boolean result = coll.remove(isBinary() ? data : entry.getSerializationService().toObject(data));
        if (coll.isEmpty()){
            entry.removeEntry();
        }
        if (result){
            entry.publishEvent(EntryEventType.REMOVED, data);
            shouldBackup = true;
        }
        return result;
    }

    public void executeBackup(Entry entry) {
        Collection coll = entry.getValue();
        if (coll == null){
            return;
        }
        coll.remove(isBinary() ? data : entry.getSerializationService().toObject(data));
        if (coll.isEmpty()){
            entry.removeEntry();
        }
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        IOUtil.writeNullableData(out, data);
    }

    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        data = IOUtil.readNullableData(in);
    }

    public boolean shouldWait(Entry entry) {
        return entry.isLocked();
    }

    public long getWaitTimeoutMillis() {
        return -1;
    }

    public Object onWaitExpire() {
        return false;
    }
}
