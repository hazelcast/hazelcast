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
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.VectorValues;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readMapStringKey;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeMapStringKey;

public class SearchOptionsImpl implements SearchOptions, IdentifiedDataSerializable {

    private boolean includePayload;
    private boolean includeVectors;
    private int limit;
    private VectorValues vectors;
    private Map<String, String> hints = Map.of();

    public SearchOptionsImpl() {
    }

    public SearchOptionsImpl(boolean includePayload, boolean includeVectors, int limit, VectorValues vectors,
                             Map<String, String> hints) {
        this.includePayload = includePayload;
        this.includeVectors = includeVectors;
        this.limit = limit;
        this.vectors = vectors;
        this.hints = hints != null ? hints : Map.of();
    }

    @Override
    public VectorValues getVectors() {
        return vectors;
    }

    @Override
    public boolean isIncludeValue() {
        return includePayload;
    }

    @Override
    public boolean isIncludeVectors() {
        return includeVectors;
    }

    @Override
    public int getLimit() {
        return limit;
    }

    @Override
    public Map<String, String> getHints() {
        return Collections.unmodifiableMap(hints);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(includePayload);
        out.writeBoolean(includeVectors);
        out.writeInt(limit);
        out.writeObject(vectors);
        writeMapStringKey(hints, out, ObjectDataOutput::writeString);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        includePayload = in.readBoolean();
        includeVectors = in.readBoolean();
        limit = in.readInt();
        vectors = in.readObject();
        hints = readMapStringKey(in, ObjectDataInput::readString);
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerConstants.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerConstants.SEARCH_OPTIONS;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SearchOptionsImpl that = (SearchOptionsImpl) o;
        return includePayload == that.includePayload && includeVectors == that.includeVectors
                && limit == that.limit && Objects.equals(vectors, that.vectors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(includePayload, includeVectors, limit, vectors);
    }
}
