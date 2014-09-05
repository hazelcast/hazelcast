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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import java.io.IOException;

public class AddIndexOperationFactory implements OperationFactory {

    String name;
    String attributeName;
    boolean ordered;

    public AddIndexOperationFactory() {
    }

    public AddIndexOperationFactory(String name, String attributeName, boolean ordered) {
        this.name = name;
        this.attributeName = attributeName;
        this.ordered = ordered;
    }

    @Override
    public Operation createOperation() {
        return new AddIndexOperation(name, attributeName, ordered);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(attributeName);
        out.writeBoolean(ordered);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        attributeName = in.readUTF();
        ordered = in.readBoolean();
    }
}
