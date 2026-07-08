/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.ops;

import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.vector.internal.impl.VectorCollectionSerializerHook;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

public class OptimizeOperationsFactory implements OperationFactory {
    private UUID uuid;
    private String vectorCollectionName;
    private String indexName;

    // used only for serialization
    public OptimizeOperationsFactory() {
    }

    public OptimizeOperationsFactory(String vectorCollectionName, String indexName) {
        this.vectorCollectionName = vectorCollectionName;
        this.indexName = indexName;
        this.uuid = UuidUtil.newUnsecureUUID();
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.OPTIMIZE_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        UUIDSerializationUtil.writeUUID(out, uuid);
        out.writeString(vectorCollectionName);
        out.writeString(indexName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.uuid = UUIDSerializationUtil.readUUID(in);
        this.vectorCollectionName = in.readString();
        this.indexName = in.readString();
    }

    @Override
    public Operation createOperation() {
        return new OptimizeOperation(uuid, vectorCollectionName, indexName);
    }

    public void setUuid(@Nonnull UUID uuid) {
        this.uuid = Objects.requireNonNull(uuid);
    }

    @Override
    public String toString() {
        return "OptimizeOperationsFactory{"
                + "uuid=" + uuid
                + ", vectorCollectionName='" + vectorCollectionName + '\''
                + ", indexName='" + indexName + '\''
                + '}';
    }
}
