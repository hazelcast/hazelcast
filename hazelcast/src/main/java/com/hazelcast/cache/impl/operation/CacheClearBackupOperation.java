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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Backup operation of {@link com.hazelcast.cache.impl.operation.CacheClearOperation}.
 * <p>It simply clears the records of all removed keys.</p>
 */
public class CacheClearBackupOperation
        extends AbstractNamedOperation
        implements BackupOperation, IdentifiedDataSerializable {

    private Set<Data> keys;

    private boolean isClear;

    private transient ICacheRecordStore cache;

    public CacheClearBackupOperation() {
    }

    public CacheClearBackupOperation(String name, Set<Data> keys, boolean isClear) {
        super(name);
        this.keys = keys;
        this.isClear = isClear;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CLEAR_BACKUP;
    }

    @Override
    public void beforeRun()
            throws Exception {
        CacheService service = getService();
        cache = service.getOrCreateCache(name, getPartitionId());
    }

    @Override
    public void run()
            throws Exception {
        if (isClear) {
            cache.clear();
            return;
        }
        if (keys != null) {
            for (Data key : keys) {
                cache.removeRecord(key);
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeBoolean(isClear);
        out.writeBoolean(keys != null);
        if (keys != null) {
            out.writeInt(keys.size());
            for (Data key : keys) {
                out.writeData(key);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        isClear = in.readBoolean();
        boolean isKeysNotNull = in.readBoolean();
        if (isKeysNotNull) {
            int size = in.readInt();
            keys = new HashSet<Data>(size);
            for (int i = 0; i < size; i++) {
                Data key = in.readData();
                keys.add(key);
            }
        }
    }

}
