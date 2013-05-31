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

package com.hazelcast.collection.operations;

import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class MultiMapOperationFactory implements OperationFactory {

    private CollectionProxyId proxyId;

    private OperationFactoryType operationFactoryType;

    private Data key;

    private Data value;

    public MultiMapOperationFactory() {
    }

    public MultiMapOperationFactory(CollectionProxyId proxyId, OperationFactoryType operationFactoryType) {
        this.proxyId = proxyId;
        this.operationFactoryType = operationFactoryType;
    }

    public MultiMapOperationFactory(CollectionProxyId proxyId, OperationFactoryType operationFactoryType, Data key, Data value) {
        this(proxyId, operationFactoryType);
        this.key = key;
        this.value = value;
    }

    public Operation createOperation() {
        if (operationFactoryType == OperationFactoryType.KEY_SET){
            return new KeySetOperation(proxyId);
        } else if (operationFactoryType == OperationFactoryType.VALUES){
            return new ValuesOperation(proxyId);
        } else if (operationFactoryType == OperationFactoryType.ENTRY_SET){
            return new EntrySetOperation(proxyId);
        } else if (operationFactoryType == OperationFactoryType.CONTAINS){
            return new ContainsEntryOperation(proxyId, key, value);
        } else if (operationFactoryType == OperationFactoryType.SIZE){
            return new SizeOperation(proxyId);
        } else if (operationFactoryType == OperationFactoryType.CLEAR){
            return new ClearOperation(proxyId);
        }

        return null;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        proxyId.writeData(out);
        out.writeInt(operationFactoryType.type);
        IOUtil.writeNullableData(out, key);
        IOUtil.writeNullableData(out, value);
    }

    public void readData(ObjectDataInput in) throws IOException {
        proxyId = new CollectionProxyId();
        proxyId.readData(in);
        operationFactoryType = OperationFactoryType.getByType(in.readInt());
        key = IOUtil.readNullableData(in);
        value = IOUtil.readNullableData(in);
    }

    public enum OperationFactoryType{
        KEY_SET(1), VALUES(2), ENTRY_SET(3), CONTAINS(4), SIZE(5), CLEAR(6);

        final int type;

        OperationFactoryType(int type) {
            this.type = type;
        }

        static OperationFactoryType getByType(int type){
            for (OperationFactoryType factoryType: values()){
                if (factoryType.type == type){
                    return factoryType;
                }
            }
            return null;
        }
    }
}
