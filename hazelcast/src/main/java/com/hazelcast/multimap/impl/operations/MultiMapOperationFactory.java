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

import com.hazelcast.nio.IOUtil;
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
        //TODO: Don't use a if/else, but use a switch case.

        if (operationFactoryType == OperationFactoryType.KEY_SET) {
            return new KeySetOperation(name);
        } else if (operationFactoryType == OperationFactoryType.VALUES) {
            return new ValuesOperation(name);
        } else if (operationFactoryType == OperationFactoryType.ENTRY_SET) {
            return new EntrySetOperation(name);
        } else if (operationFactoryType == OperationFactoryType.CONTAINS) {
            return new ContainsEntryOperation(name, key, value, threadId);
        } else if (operationFactoryType == OperationFactoryType.SIZE) {
            return new SizeOperation(name);
        } else if (operationFactoryType == OperationFactoryType.CLEAR) {
            return new ClearOperation(name);
        }

        return null;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(operationFactoryType.type);
        out.writeLong(threadId);
        IOUtil.writeNullableData(out, key);
        IOUtil.writeNullableData(out, value);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        operationFactoryType = OperationFactoryType.getByType(in.readInt());
        threadId = in.readLong();
        key = IOUtil.readNullableData(in);
        value = IOUtil.readNullableData(in);
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
