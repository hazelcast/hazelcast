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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;

import java.io.IOException;
import java.util.Objects;

public class VectorDocumentImpl<V> implements VectorDocument<V>, IdentifiedDataSerializable {

    private V userValue;
    private VectorValues vectorValues;

    public VectorDocumentImpl() {
    }

    public VectorDocumentImpl(V userValue, VectorValues vectorValues) {
        this.userValue = userValue;
        this.vectorValues = vectorValues;
    }

    @Override
    public V getValue() {
        return userValue;
    }

    @Override
    public VectorValues getVectors() {
        return vectorValues;
    }

    public VectorDocument<V> value(V userValue) {
        this.userValue = userValue;
        return this;
    }

    public VectorDocument<V> vectorValues(VectorValues vectorValues) {
        this.vectorValues = vectorValues;
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(userValue);
        out.writeObject(vectorValues);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        userValue = in.readObject();
        vectorValues = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerConstants.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerConstants.VECTOR_DOCUMENT;
    }

    @Override
    public String toString() {
        return "VectorDocumentImpl{"
                + "userValue=" + userValue
                + ", vectorValues=" + vectorValues
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VectorDocumentImpl<?> that = (VectorDocumentImpl<?>) o;
        return Objects.equals(userValue, that.userValue) && Objects.equals(vectorValues, that.vectorValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userValue, vectorValues);
    }
}
