/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.extract.GenericQueryTarget;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.io.IOException;

/**
 * Target that is deserialized lazily.
 * <p>
 * Initially the instance is linked to {@link GenericQueryTarget} which helps us avoid the possible double-deserialization.
 * The object is unlinked
 */
public class LazyTarget implements IdentifiedDataSerializable {

    private Data serialized;
    private Object deserialized;

    public LazyTarget() {
        // No-op.
    }

    public LazyTarget(Data serialized, Object deserialized) {
        assert serialized != null || deserialized != null;

        this.serialized = serialized;
        this.deserialized = deserialized;
    }

    public Object deserialize(SerializationService serializationService) {
        try {
            if (deserialized == null) {
                deserialized = serializationService.toObject(serialized);
            }

            QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(deserialized.getClass());

            return type.normalize(deserialized);
        } catch (Exception e) {
            throw QueryException.dataException("Failed to deserialize map entry key or value: " + e.getMessage(), e);
        }
    }

    public Data getSerialized() {
        return serialized;
    }

    public void setSerialized(Data serialized) {
        this.serialized = serialized;
    }

    public Object getDeserialized() {
        return deserialized;
    }

    public void setDeserialized(Object deserialized) {
        this.deserialized = deserialized;
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.LAZY_TARGET;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        if (serialized != null) {
            out.writeBoolean(true);
            out.writeByteArray(serialized.toByteArray());
        } else {
            out.writeBoolean(false);
            out.writeObject(deserialized);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        if (in.readBoolean()) {
            serialized = new HeapData(in.readByteArray());
        } else {
            deserialized = in.readObject();
        }
    }
}
