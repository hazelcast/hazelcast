/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.sql.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Arrays;

/**
 * A wrapper for Object[] supporting equals/hashCode and hz-serialization.
 */
public final class ObjectArrayKey implements DataSerializable {

    private Object[] array;

    @SuppressWarnings("unused")
    private ObjectArrayKey() {
    }

    private ObjectArrayKey(Object[] array) {
        this.array = array;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(array);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        array = in.readObject();
    }

    @Override
    public String toString() {
        return "ObjectArray{" +
                "array=" + Arrays.toString(array) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ObjectArrayKey that = (ObjectArrayKey) o;
        return Arrays.equals(array, that.array);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(array);
    }

    public static ObjectArrayKey project(Object[] row, int[] indices) {
        Object[] key = new Object[indices.length];
        for (int i = 0; i < indices.length; i++) {
            key[i] = row[indices[i]];
        }
        return new ObjectArrayKey(key);
    }

    /**
     * Return a function that maps an input `Object[]` to an {@link
     * ObjectArrayKey}, extracting the fields given in {@code indices}.
     *
     * @param indices the indices of keys
     * @return the projection function
     */
    public static FunctionEx<Object[], ObjectArrayKey> projectFn(int[] indices) {
        return row -> project(row, indices);
    }
}
