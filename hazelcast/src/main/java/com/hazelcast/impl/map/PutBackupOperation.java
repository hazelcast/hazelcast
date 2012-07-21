/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.map;

import com.hazelcast.impl.DefaultRecord;
import com.hazelcast.impl.Record;
import com.hazelcast.impl.spi.NonBlockingOperation;
import com.hazelcast.impl.spi.OperationContext;
import com.hazelcast.impl.spi.Response;
import com.hazelcast.nio.Data;

public class PutBackupOperation extends PutOperation implements NonBlockingOperation {

    public PutBackupOperation(String name, Data dataKey, Data dataValue, long ttl) {
        super(name, dataKey, dataValue, ttl);
    }

    public PutBackupOperation() {
    }

    public Object call() {
        OperationContext context = getOperationContext();
        MapService mapService = (MapService) context.getService();
        MapPartition mapPartition = mapService.getMapPartition(context.getPartitionId(), name);
        Record record = mapPartition.records.get(dataKey);
        if (record == null) {
            record = new DefaultRecord(null, mapPartition.partitionInfo.getPartitionId(), dataKey, dataValue, -1, -1, mapService.nextId());
            mapPartition.records.put(dataKey, record);
        } else {
            record.setValueData(dataValue);
        }
        record.setActive();
        record.setDirty(true);
        return new Response();
    }

    @Override
    public String toString() {
        return "PutBackupOperation{}";
    }
}
