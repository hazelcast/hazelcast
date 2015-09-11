/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.merge.entry.CacheEntryView;
import com.hazelcast.cache.impl.merge.policy.CacheMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

public class CacheMergeOperation
        extends AbstractCacheOperation
        implements BackupAwareOperation, MutatingOperation {

    private CacheMergePolicy mergePolicy;
    private CacheEntryView<Data, Data> mergingEntry;

    public CacheMergeOperation() {
    }

    public CacheMergeOperation(String name, Data key, CacheEntryView<Data, Data> entryView, CacheMergePolicy policy) {
        super(name, key);
        mergingEntry = entryView;
        mergePolicy = policy;
    }

    @Override
    public void run() throws Exception {
        backupRecord = cache.merge(mergingEntry, mergePolicy);
    }

    @Override
    public boolean shouldBackup() {
        return backupRecord != null;
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutBackupOperation(name, key, backupRecord);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mergingEntry);
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mergingEntry = in.readObject();
        mergePolicy = in.readObject();
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.MERGE;
    }

}
