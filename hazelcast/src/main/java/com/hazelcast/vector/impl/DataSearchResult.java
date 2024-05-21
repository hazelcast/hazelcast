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
import com.hazelcast.vector.VectorValues;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

public class DataSearchResult implements InternalSearchResult<Data, Data>, IdentifiedDataSerializable {

    private Data key;
    private float score;
    // internal vector index id
    private int id;
    @Nullable
    private Data value;
    @Nullable
    private VectorValues vectors;

    public DataSearchResult() {
    }

    public DataSearchResult(int id, Data key, float score) {
        this.id = id;
        this.key = key;
        this.score = score;
    }

    // used only by client protocol codecs
    public DataSearchResult(Data key, Data value, float score, VectorValues vectors) {
        this.id = -1;
        this.key = key;
        this.score = score;
        this.value = value;
        this.vectors = vectors;
    }

    @Override
    public DataSearchResult setValue(Data value) {
        this.value = value;
        return this;
    }

    @Override
    public DataSearchResult setVectors(VectorValues vectors) {
        this.vectors = vectors;
        return this;
    }

    @Nonnull
    @Override
    public Data getKey() {
        return key;
    }

    @Nullable
    @Override
    public Data getValue() {
        return value;
    }

    @Nullable
    @Override
    public VectorValues getVectors() {
        return vectors;
    }

    @Override
    public float getScore() {
        return score;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeData(out, key);
        out.writeFloat(score);
        IOUtil.writeData(out, value);
        out.writeObject(vectors);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = IOUtil.readData(in);
        score = in.readFloat();
        value = IOUtil.readData(in);
        vectors = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerConstants.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerConstants.DATA_SEARCH_RESULT;
    }

    @Override
    public String toString() {
        return "DataSearchResult{"
                + "key=" + key
                + ", score=" + score
                + ", value=" + value
                + ", vectors=" + vectors
                + '}';
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataSearchResult that = (DataSearchResult) o;
        return Float.compare(score, that.score) == 0 && id == that.id
                && Objects.equals(key, that.key) && Objects.equals(value, that.value)
                && Objects.equals(vectors, that.vectors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }
}
