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

package com.hazelcast.map;

import com.hazelcast.impl.DefaultRecord;
import com.hazelcast.nio.Data;

import static com.hazelcast.nio.IOUtil.toObject;

public class PutIfAbsentOperation extends BasePutOperation {

    boolean absent = true;

    public PutIfAbsentOperation(String name, Data dataKey, Data value, String txnId, long ttl) {
        super(name, dataKey, value, txnId, ttl);
    }

    public PutIfAbsentOperation() {
    }

    @Override
    void initFlags() {
        // use default flags
    }


    protected void prepareRecord() {
        record = mapPartition.records.get(dataKey);
        if (record == null) {
            load();
            if (absent) {
                record = new DefaultRecord(getPartitionId(), dataKey, dataValue, -1, -1, mapService.nextId());
                mapPartition.records.put(dataKey, record);
                record.setActive();
                record.setDirty(true);
            }
        } else {
            oldValueData = record.getValueData();
            absent = false;
        }
    }

    protected void load() {
        if (mapPartition.loader != null) {
            if (key == null) {
                key = toObject(dataKey);
            }
            Object oldValue = mapPartition.loader.load(key);
            absent = oldValue == null;
        }
    }

    public void doOp() {
        init();
        // todo transaction should be written relating to if absent
        if (prepareTransaction()) {
            return;
        }
        prepareRecord();
        if (absent) {
            store();
//            sendBackups();
        }
//        sendResponse();
    }


    @Override
    public String toString() {
        return "PutIfAbsentOperation{" + name + "}";
    }
}
