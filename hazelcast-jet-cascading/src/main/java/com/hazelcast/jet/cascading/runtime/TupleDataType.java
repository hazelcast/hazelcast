/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cascading.runtime;

import cascading.tuple.Tuple;
import com.hazelcast.jet.io.DataType;
import com.hazelcast.jet.io.SerializationOptimizer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class TupleDataType implements DataType {

    public static final byte TYPE_ID = 1;

    public static final DataType INSTANCE = new TupleDataType();

    @Override
    public Class getClazz() {
        return Tuple.class;
    }

    @Override
    public byte typeId() {
        return TYPE_ID;
    }

    @Override
    public void write(Object o, ObjectDataOutput objectDataOutput, SerializationOptimizer optimizer) throws IOException {
        objectDataOutput.write(TYPE_ID);
        Tuple tuple = (Tuple) o;
        int size = tuple.size();
        objectDataOutput.writeInt(size);
        for (int i = 0; i < size; i++) {
            optimizer.write(tuple.getObject(i), objectDataOutput);
        }
    }

    @Override
    public Object read(ObjectDataInput objectDataInput, SerializationOptimizer optimizer) throws IOException {
        int size = objectDataInput.readInt();
        Tuple tuple = Tuple.size(size);
        for (int i = 0; i < size; i++) {
            tuple.set(i, optimizer.read(objectDataInput));
        }
        return tuple;
    }
}
