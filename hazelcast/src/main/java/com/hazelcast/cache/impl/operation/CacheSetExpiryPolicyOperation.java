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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CacheSetExpiryPolicyOperation extends CacheOperation
        implements MutatingOperation, BackupAwareOperation {

    private List<Data> keys;
    private Data expiryPolicy;
    private transient boolean response;

    public CacheSetExpiryPolicyOperation() {

    }

    public CacheSetExpiryPolicyOperation(String name, List<Data> keys, Data expiryPolicy) {
        super(name);
        this.keys = keys;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    public void run() throws Exception {
        if (recordStore == null) {
            return;
        }
        response = recordStore.setExpiryPolicy(keys, expiryPolicy, getCallerUuid());
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        if (recordStore.isWanReplicationEnabled()) {
            for (Data key : keys) {
                CacheRecord record = recordStore.getRecord(key);
                publishWanUpdate(key, record);
            }
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.SET_EXPIRY_POLICY;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(keys.size());
        for (Data key: keys) {
            IOUtil.writeData(out, key);
        }
        IOUtil.writeData(out, expiryPolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int s = in.readInt();
        keys = new ArrayList<Data>(s);
        while (s-- > 0) {
            keys.add(IOUtil.readData(in));
        }
        expiryPolicy = IOUtil.readData(in);
    }

    @Override
    public boolean shouldBackup() {
        return recordStore.getConfig().getTotalBackupCount() > 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new CacheSetExpiryPolicyBackupOperation(name, keys, expiryPolicy);
    }

    @Override
    public boolean requiresTenantContext() {
        return true;
    }
}
