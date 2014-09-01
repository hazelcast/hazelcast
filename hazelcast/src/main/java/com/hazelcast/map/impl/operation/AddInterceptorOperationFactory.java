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

import com.hazelcast.map.impl.MapInterceptor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import java.io.IOException;

public class AddInterceptorOperationFactory implements OperationFactory {

    String id;
    String name;
    MapInterceptor mapInterceptor;

    public AddInterceptorOperationFactory() {
    }

    public AddInterceptorOperationFactory(String id, String name, MapInterceptor mapInterceptor) {
        this.id = id;
        this.name = name;
        this.mapInterceptor = mapInterceptor;
    }

    @Override
    public Operation createOperation() {
        return new AddInterceptorOperation(id, mapInterceptor, name);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(id);
        out.writeObject(mapInterceptor);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        id = in.readUTF();
        mapInterceptor = in.readObject();
    }
}
