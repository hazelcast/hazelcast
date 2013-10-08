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

package com.hazelcast.map.operation;

import com.hazelcast.map.EntryMapper;

import com.hazelcast.map.EntryReducer;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class MapCombineOperationFactory implements OperationFactory {

    String name;
    EntryMapper mapper;
    EntryReducer combiner;

    public MapCombineOperationFactory() {
    }

    public MapCombineOperationFactory(String name, EntryMapper mapper, EntryReducer combiner) {
        this.name = name;
        this.mapper = mapper;
        this.combiner = combiner;
    }

    @Override
    public Operation createOperation() {
        return new MapCombineOperation(name, mapper, combiner);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(mapper);
        out.writeObject(combiner);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        mapper = in.readObject();
        combiner = in.readObject();
    }
}
