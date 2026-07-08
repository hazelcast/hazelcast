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

package com.hazelcast.vector.internal.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.impl.InternalSearchResult;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

public class SearchResultImpl<K, V> implements InternalSearchResult<K, V>, IdentifiedDataSerializable {

    private K key;

    private float score;

    @Nullable
    private V value;

    @Nullable
    private VectorValues vectors;

    // internal vector index id
    private transient int id;

    public SearchResultImpl() {
    }

    public SearchResultImpl(int id, @Nonnull K key, float score) {
        this.id = id;
        this.key = key;
        this.score = score;
    }

    @Override
    public SearchResultImpl<K, V> setValue(V value) {
        this.value = value;
        return this;
    }

    @Override
    public SearchResultImpl<K, V> setVectors(VectorValues vectors) {
        this.vectors = vectors;
        return this;
    }

    @Nonnull
    @Override
    public K getKey() {
        return key;
    }

    @Nullable
    @Override
    public V getValue() {
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
        out.writeObject(key);
        out.writeFloat(score);
        out.writeObject(value);
        out.writeObject(vectors);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = in.readObject();
        score = in.readFloat();
        value = in.readObject();
        vectors = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.SEARCH_RESULT;
    }

    @Override
    public String toString() {
        return "SearchResultImpl{"
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
        SearchResultImpl<?, ?> that = (SearchResultImpl<?, ?>) o;
        return Float.compare(score, that.score) == 0 && id == that.id
                && Objects.equals(key, that.key) && Objects.equals(value, that.value)
                && Objects.equals(vectors, that.vectors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, score, value, vectors, id);
    }
}
