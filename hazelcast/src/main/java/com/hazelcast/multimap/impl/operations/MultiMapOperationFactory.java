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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import java.io.IOException;

public class MultiMapOperationFactory implements OperationFactory {

    private String name;

    private OperationFactoryType operationFactoryType;

    private Data key;

    private Data value;

    private long threadId;

    public MultiMapOperationFactory() {
    }

    public MultiMapOperationFactory(String name, OperationFactoryType operationFactoryType) {
        this.name = name;
        this.operationFactoryType = operationFactoryType;
    }

    public MultiMapOperationFactory(String name, OperationFactoryType operationFactoryType, Data key, Data value) {
        this(name, operationFactoryType);
        this.key = key;
        this.value = value;
    }

    public MultiMapOperationFactory(String name, OperationFactoryType operationFactoryType, Data key, Data value, long threadId) {
        this(name, operationFactoryType);
        this.key = key;
        this.value = value;
        this.threadId = threadId;
    }

    public Operation createOperation() {

        switch(operationFactoryType) {
            case KEY_SET:
                 return new KeySetOperation(name);
            case VALUES:
                 return new ValuesOperation(name);
            case ENTRY_SET:
                 return new EntrySetOperation(name);
            case CONTAINS:
                 return new ContainsEntryOperation(name, key, value, threadId);
            case SIZE:
                 return new SizeOperation(name);
            case CLEAR:
                 return new ClearOperation(name);
            default:
                 return null;
        }
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(operationFactoryType.type);
        out.writeLong(threadId);
        out.writeData(key);
        out.writeData(value);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        operationFactoryType = OperationFactoryType.getByType(in.readInt());
        threadId = in.readLong();
        key = in.readData();
        value = in.readData();
    }

    public enum OperationFactoryType {
        KEY_SET(1), VALUES(2), ENTRY_SET(3), CONTAINS(4), SIZE(5), CLEAR(6);

        final int type;

        OperationFactoryType(int type) {
            this.type = type;
        }

        static OperationFactoryType getByType(int type) {
            for (OperationFactoryType factoryType : values()) {
                if (factoryType.type == type) {
                    return factoryType;
                }
            }
            return null;
        }
    }
}
