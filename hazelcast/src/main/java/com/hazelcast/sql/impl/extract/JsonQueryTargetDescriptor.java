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

package com.hazelcast.sql.impl.extract;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.SqlDataSerializerHook;

import java.io.IOException;
import java.util.Set;

public class JsonQueryTargetDescriptor implements QueryTargetDescriptor, IdentifiedDataSerializable {

    private Set<String> staticallyTypedPaths;

    @SuppressWarnings("unused")
    public JsonQueryTargetDescriptor() {
    }

    public JsonQueryTargetDescriptor(Set<String> staticallyTypedPaths) {
        this.staticallyTypedPaths = staticallyTypedPaths;
    }

    @Override
    public QueryTarget create(InternalSerializationService serializationService, Extractors extractors, boolean isKey) {
        return new JsonQueryTarget(staticallyTypedPaths, extractors, isKey);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.TARGET_DESCRIPTOR_JSON;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(staticallyTypedPaths);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        staticallyTypedPaths = in.readObject();
    }
}
