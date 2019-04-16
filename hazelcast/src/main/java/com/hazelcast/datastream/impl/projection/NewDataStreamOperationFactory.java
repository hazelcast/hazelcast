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

import com.hazelcast.datastream.impl.DSDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NewDataStreamOperationFactory implements OperationFactory {

    private String recordType;
    private String srcName;
    private String targetName;
    private String preparationId;
    private Map<String, Object> bindings;

    public NewDataStreamOperationFactory() {
    }

    public NewDataStreamOperationFactory(String srcName,
                                         String targetName,
                                         String recordType,
                                         String preparationId,
                                         Map<String, Object> bindings) {
        this.recordType = recordType;
        this.srcName = srcName;
        this.targetName = targetName;
        this.preparationId = preparationId;
        this.bindings = bindings;
    }

    @Override
    public Operation createOperation() {
        return new NewDataStreamOperation(srcName, targetName, recordType, preparationId, bindings);
    }

    @Override
    public int getFactoryId() {
        return DSDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DSDataSerializerHook.NEW_DATASTREAM_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(srcName);
        out.writeUTF(targetName);
        out.writeUTF(recordType);
        out.writeUTF(preparationId);
        out.writeInt(bindings.size());
        for (Map.Entry<String, Object> entry : bindings.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        srcName = in.readUTF();
        targetName = in.readUTF();
        recordType = in.readUTF();
        preparationId = in.readUTF();
        int size = in.readInt();
        bindings = new HashMap<String, Object>();
        for (int k = 0; k < size; k++) {
            bindings.put(in.readUTF(), in.readObject());
        }
    }
}