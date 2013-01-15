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

import com.hazelcast.collection.processor.BaseEntryProcessor;
import com.hazelcast.config.MultiMapConfig.ValueCollectionType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * @ali 1/3/13
 */
public abstract class MultiMapEntryProcessor<T> extends BaseEntryProcessor<T> {

    ValueCollectionType collectionType = ValueCollectionType.SET;

    int syncBackupCount;

    int asyncBackupCount;

    transient boolean shouldBackup = false;

    protected MultiMapEntryProcessor() {
    }

    protected MultiMapEntryProcessor(boolean binary){
        super(binary);
    }

    protected MultiMapEntryProcessor(boolean binary, ValueCollectionType collectionType) {
        super(binary);
        this.collectionType = collectionType;
    }

    public int getSyncBackupCount() {
        return syncBackupCount;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public boolean shouldBackup() {
        return shouldBackup;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeUTF(collectionType.toString());
        out.writeInt(syncBackupCount);
        out.writeInt(asyncBackupCount);
    }

    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        collectionType = ValueCollectionType.valueOf(in.readUTF());
        syncBackupCount = in.readInt();
        asyncBackupCount = in.readInt();
    }
}
