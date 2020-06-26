/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Map;

public class PojoUpsertTargetDescriptor implements UpsertTargetDescriptor {

    private String className;
    private Map<String, String> typeNamesByFields;

    @SuppressWarnings("unused")
    PojoUpsertTargetDescriptor() {
    }

    public PojoUpsertTargetDescriptor(String className, Map<String, String> typeNamesByFields) {
        this.className = className;
        this.typeNamesByFields = typeNamesByFields;
    }

    @Override
    public UpsertTarget create(InternalSerializationService serializationService) {
        return new PojoUpsertTarget(className, typeNamesByFields);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(className);
        out.writeObject(typeNamesByFields);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        className = in.readUTF();
        typeNamesByFields = in.readObject();
    }
}
