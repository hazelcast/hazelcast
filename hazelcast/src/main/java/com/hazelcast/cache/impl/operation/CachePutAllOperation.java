/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

public class CachePutAllOperation extends CacheOperation
        implements BackupAwareOperation, MutableOperation, MutatingOperation {

    private List<Map.Entry<Data, Data>> entries;
    private ExpiryPolicy expiryPolicy;
    private int completionId;

    private transient Map<Data, CacheRecord> backupRecords;

    public CachePutAllOperation() {
    }

    public CachePutAllOperation(String cacheNameWithPrefix, List<Map.Entry<Data, Data>> entries, ExpiryPolicy expiryPolicy,
                                int completionId) {
        super(cacheNameWithPrefix);
        this.entries = entries;
        this.expiryPolicy = expiryPolicy;
        this.completionId = completionId;
    }

    @Override
    public int getCompletionId() {
        return completionId;
    }

    @Override
    public void setCompletionId(int completionId) {
        this.completionId = completionId;
    }

    @Override
    public void run() throws Exception {
        UUID callerUuid = getCallerUuid();
        backupRecords = createHashMap(entries.size());

        for (Map.Entry<Data, Data> entry : entries) {
            Data key = entry.getKey();
            Data value = entry.getValue();

            CacheRecord backupRecord = recordStore.put(key, value, expiryPolicy, callerUuid, completionId);

            // backupRecord may be null (eg expired on put)
            if (backupRecord != null) {
                backupRecords.put(key, backupRecord);
                publishWanUpdate(key, backupRecord);
            }
        }
    }

    @Override
    public boolean shouldBackup() {
        return !backupRecords.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutAllBackupOperation(name, backupRecords);
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.PUT_ALL;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(expiryPolicy);
        out.writeInt(completionId);
        out.writeInt(entries.size());
        for (Map.Entry<Data, Data> entry : entries) {
            IOUtil.writeData(out, entry.getKey());
            IOUtil.writeData(out, entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expiryPolicy = in.readObject();
        completionId = in.readInt();
        int size = in.readInt();
        entries = new ArrayList<Map.Entry<Data, Data>>(size);
        for (int i = 0; i < size; i++) {
            Data key = IOUtil.readData(in);
            Data value = IOUtil.readData(in);
            entries.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(key, value));
        }
    }

    @Override
    public boolean requiresTenantContext() {
        return true;
    }
}
