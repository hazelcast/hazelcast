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
import com.hazelcast.impl.Record;
import com.hazelcast.nio.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PutBackupOperation extends PutOperation {

    boolean sendResponse = true;

    public PutBackupOperation(String name, Data dataKey, Data dataValue, long ttl) {
        this(name, dataKey, dataValue, ttl, true);
    }

    public PutBackupOperation(String name, Data dataKey, Data dataValue, long ttl, boolean sendResponse) {
        super(name, dataKey, dataValue, null, ttl);
        this.sendResponse = sendResponse;
    }

    public PutBackupOperation() {
    }

    public void run() {
        MapService mapService = (MapService) getService();
        System.out.println(getNodeEngine().getThisAddress() + " backup " + txnId + " response " + sendResponse);
        DefaultRecordStore mapPartition = mapService.getMapPartition(getPartitionId(), name);
        Record record = mapPartition.records.get(dataKey);
        if (record == null) {
            record = new DefaultRecord( mapService.nextId(), dataKey, dataValue, -1, -1);
            mapPartition.records.put(dataKey, record);
        } else {
            record.setValueData(dataValue);
        }
        record.setActive(true);
        record.setDirty(true);
        if (sendResponse) getResponseHandler().sendResponse(null);
    }

    @Override
    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(sendResponse);
    }

    @Override
    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        sendResponse = in.readBoolean();
    }

    @Override
    public String toString() {
        return "PutBackupOperation{}";
    }
}
