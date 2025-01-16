/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.vector.VectorValues;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public final class MultiIndexVectorValues
        implements VectorValues.MultiIndexVectorValues, IdentifiedDataSerializable {
    private Map<String, float[]> indexNameToVector;

    public MultiIndexVectorValues() {
    }

    public MultiIndexVectorValues(@Nonnull Map<String, float[]> indexNameToVector) {
        this.indexNameToVector = Objects.requireNonNull(indexNameToVector, "indexNameToVector");
    }

    @Override
    public Map<String, float[]> indexNameToVector() {
        return indexNameToVector;
    }

    @Override
    public String toString() {
        return "MultiIndexVectorValues{"
                + "indexNameToVector=" + VectorStringUtil.mapToString(indexNameToVector)
                + '}';
    }

    public int size() {
        return indexNameToVector.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MultiIndexVectorValues that = (MultiIndexVectorValues) o;
        var m = that.indexNameToVector();

        if (m.size() != indexNameToVector.size()) {
            return false;
        }

        try {
            for (Map.Entry<String, float[]> e : indexNameToVector.entrySet()) {
                var key = e.getKey();
                var value = e.getValue();
                if (value == null) {
                    if (!(m.get(key) == null && m.containsKey(key))) {
                        return false;
                    }
                } else {
                    if (!Arrays.equals(value, m.get(key))) {
                        return false;
                    }
                }
            }
        } catch (ClassCastException | NullPointerException unused) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexNameToVector);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        SerializationUtil.writeMapStringKey(indexNameToVector, out, ObjectDataOutput::writeFloatArray);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        indexNameToVector = SerializationUtil.readMapStringKey(in, ObjectDataInput::readFloatArray);
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerConstants.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerConstants.MULTIPLE_VECTOR_VALUES;
    }
}
