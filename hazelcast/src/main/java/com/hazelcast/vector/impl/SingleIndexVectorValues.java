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
import com.hazelcast.vector.VectorValues;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;

public final class SingleIndexVectorValues implements VectorValues.SingleVectorValues, IdentifiedDataSerializable {
    private float[] vector;

    public SingleIndexVectorValues() {
    }

    public SingleIndexVectorValues(@Nonnull float[] vector) {
        this.vector = vector;
    }

    @Override
    @Nonnull
    public float[] vector() {
        return vector;
    }

    @Override
    public String toString() {
        return "SingleIndexVectorValues{"
                + "vector=" + Arrays.toString(vector)
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SingleIndexVectorValues that)) {
            return false;
        }
        return Arrays.equals(vector, that.vector);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(vector);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeFloatArray(vector);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        vector = in.readFloatArray();
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerConstants.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerConstants.SINGLE_VECTOR_VALUES;
    }
}
