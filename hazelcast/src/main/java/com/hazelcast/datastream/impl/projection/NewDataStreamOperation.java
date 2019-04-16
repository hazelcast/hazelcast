/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl.projection;

import com.hazelcast.config.DataStreamConfig;
import com.hazelcast.datastream.impl.DSDataSerializerHook;
import com.hazelcast.datastream.impl.DSPartition;
import com.hazelcast.datastream.impl.operations.DataStreamOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NewDataStreamOperation extends DataStreamOperation {

    private Map<String, Object> bindings;
    private String dstName;
    private String preparationId;
    private String recordType;

    public NewDataStreamOperation() {
    }

    public NewDataStreamOperation(String srcName,
                                  String dstName,
                                  String recordType,
                                  String preparationId,
                                  Map<String, Object> bindings) {
        super(srcName);
        this.dstName = dstName;
        this.recordType = recordType;
        this.preparationId = preparationId;
        this.bindings = bindings;
    }

    @Override
    public void run() throws Exception {
        DSPartition srcPartition = partition;
        DataStreamConfig srcConfig = srcPartition.config();

        // the dst configuration isn't optimal. But will do for now (large enough the hold the original payload)
        DataStreamConfig dstConfig = new DataStreamConfig(dstName)
                .setValueClass(NewDataStreamOperation.class.getClassLoader().loadClass(recordType))
                .setInitialRegionSize(srcConfig.getMaxRegionSize())
                .setMaxRegionSize(srcConfig.getMaxRegionSize())
                .setMaxRegionsPerPartition(Integer.MAX_VALUE);

        // in theory the size can be calculated based on record count + projection-record size
        DSPartition dstPartition = service
                .getDataStreamContainer(dstName, dstConfig)
                .getPartition(getPartitionId());

        srcPartition.executeProjectionPartitionThread(preparationId, bindings, o -> dstPartition.append(o));
        dstPartition.freeze();
    }

    @Override
    public int getId() {
        return DSDataSerializerHook.NEW_DATASTREAM_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(dstName);
        out.writeUTF(recordType);
        out.writeUTF(preparationId);
        out.writeInt(bindings.size());
        for (Map.Entry<String, Object> entry : bindings.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        dstName = in.readUTF();
        recordType = in.readUTF();
        preparationId = in.readUTF();
        int size = in.readInt();
        bindings = new HashMap<>(size);
        for (int k = 0; k < size; k++) {
            bindings.put(in.readUTF(), in.readObject());
        }
    }
}
