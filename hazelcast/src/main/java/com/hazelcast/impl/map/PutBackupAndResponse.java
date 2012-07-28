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
import com.hazelcast.impl.spi.OperationContext;
import com.hazelcast.impl.spi.Response;
import com.hazelcast.nio.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PutBackupAndResponse extends Response {
    String name;
    Data key;
    Data value;

    public PutBackupAndResponse(Object result, String name, Data key, Data value) {
        super(result);
        this.name = name;
        this.key = key;
        this.value = value;
    }

    public PutBackupAndResponse() {
    }

    public void run() {
        OperationContext context = getOperationContext();
        MapService service = (MapService) context.getService();
        System.out.println(context.getNodeService().getThisAddress() + " backupAndResponse ");
        MapPartition mapPartition = service.getMapPartition(context.getPartitionId(), name);
        Record record = mapPartition.records.get(key);
        if (record == null) {
            record = new DefaultRecord(null, mapPartition.partitionInfo.getPartitionId(), key, value, -1, -1, service.nextId());
            mapPartition.records.put(key, record);
        } else {
            record.setValueData(value);
        }
        record.setActive();
        record.setDirty(true);
        super.run();
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        super.writeData(out);
        key.writeData(out);
        value.writeData(out);
        out.writeUTF(name);
    }

    @Override
    public void readData(DataInput in) throws IOException {
        super.readData(in);
        key = new Data();
        key.readData(in);
        value = new Data();
        value.readData(in);
        name = in.readUTF();
    }
}
