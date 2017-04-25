/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.map.impl.MapDataSerializerHook.TOUCH_KEYS_ON_BACKUPS;
import static java.util.Collections.emptyMap;

/**
 *
 */
public class TouchKeysOnBackups extends MapOperation implements BackupOperation {

    private Map<Data, RecordInfo> keysInfo;

    public TouchKeysOnBackups() {
    }

    public TouchKeysOnBackups(String name, Map<Data, RecordInfo> keysInfo) {
        super(name);
        this.keysInfo = keysInfo;
    }

    @Override
    public int getId() {
        return TOUCH_KEYS_ON_BACKUPS;
    }

    @Override
    public void run() throws Exception {
        recordStore.updateEntryMetadata(keysInfo);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeInt(keysInfo.size());
        for (Map.Entry<Data, RecordInfo> keyInfo : keysInfo.entrySet()) {
            out.writeData(keyInfo.getKey());
            out.writeLong(keyInfo.getValue().getLastAccessTime());
            out.writeLong(keyInfo.getValue().getHits());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        if (size > 0) {
            keysInfo = new HashMap<Data, RecordInfo>(size);
            for (int i = 0; i < size; i++) {
                Data key = in.readData();
                long lastAccessTime = in.readLong();
                long hits = in.readLong();
                RecordInfo info = new RecordInfo();
                info.setHits(hits);
                info.setLastAccessTime(lastAccessTime);
                keysInfo.put(key, info);
            }
        } else {
            keysInfo = emptyMap();
        }
    }
}
