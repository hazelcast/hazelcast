/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Objects;

public class DataVectorDocument implements VectorDocument<Data>, IdentifiedDataSerializable {

    private Data userValue;
    private VectorValues vectorValues;

    public DataVectorDocument() {
    }

    public DataVectorDocument(@Nonnull Data userValue, @Nonnull VectorValues vectorValues) {
        this.userValue = Objects.requireNonNull(userValue, "userValue cannot be null");
        this.vectorValues = Objects.requireNonNull(vectorValues, "vectorValues cannot be null");
    }

    @Override
    @Nonnull
    public Data getValue() {
        return userValue;
    }

    @Override
    @Nonnull
    public VectorValues getVectors() {
        return vectorValues;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeData(out, userValue);
        out.writeObject(vectorValues);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        userValue = IOUtil.readData(in);
        vectorValues = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerConstants.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerConstants.DATA_VECTOR_DOCUMENT;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataVectorDocument that = (DataVectorDocument) o;
        return Objects.equals(userValue, that.userValue) && Objects.equals(vectorValues, that.vectorValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userValue, vectorValues);
    }
}
