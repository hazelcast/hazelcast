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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import java.io.IOException;

public class MapPutAllOperationFactory implements OperationFactory {

    String name;
    MapEntrySet entrySet = new MapEntrySet();


    public MapPutAllOperationFactory() {
    }

    public MapPutAllOperationFactory(String name, MapEntrySet entrySet) {
        this.name = name;
        this.entrySet = entrySet;
    }

    @Override
    public Operation createOperation() {
        PutAllOperation putAllOperation = new PutAllOperation(name, entrySet);
        putAllOperation.setServiceName(MapService.SERVICE_NAME);
        return putAllOperation;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(entrySet);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        entrySet = in.readObject();
    }
}
