/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.List;

/**
 * Definition of an attribute of a sorted index.
 *
 * @see SortedIndexConfig
 * @see MapConfig#setIndexConfigs(List)
 */
public class SortedIndexAttribute implements IdentifiedDataSerializable {
    /** Default sort order of the attribute. */
    public static final boolean DEFAULT_ASC = true;

    /** Name of the attribute. */
    private String name;

    /** Whether the attribute is sorted in ascending order. */
    private boolean asc = DEFAULT_ASC;

    public SortedIndexAttribute() {
        // No-op.
    }

    /**
     * Creates an index attribute with the given name sorted in ascending order.
     *
     * @param name Name of the attribute.
     */
    public SortedIndexAttribute(String name) {
        this.name = name;
    }

    /**
     * Creates an index attribute with the given name and sort order.
     *
     * @param name Name of the attribute.
     * @param asc {@code True} if the attribute should be sorted in ascending order, {@code false} otherwise.
     */
    public SortedIndexAttribute(String name, boolean asc) {
        this.name = name;
        this.asc = asc;
    }

    /**
     * Gets name of the attribute.
     *
     * @return Name of the attribute.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets name of the attribute.
     *
     * @param name Name of the attribute.
     * @return This instance for chaining.
     */
    public SortedIndexAttribute setName(String name) {
        this.name = name;

        return this;
    }

    /**
     * Gets whether the attribute is sorted in ascending order.
     * <p>
     * Defaults to {@link #DEFAULT_ASC}.
     *
     * @return {@code True} if the attribute is sorted in ascending order, {@code false} otherwise.
     */
    public boolean isAsc() {
        return asc;
    }

    /**
     * Sets whether the attribute is sorted in ascending order.
     * <p>
     * Defaults to {@link #DEFAULT_ASC}.
     *
     * @param asc {@code True} if the attribute is sorted in ascending order, {@code false} otherwise.
     * @return This instance for chaining.
     */
    public SortedIndexAttribute setAsc(boolean asc) {
        this.asc = asc;

        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.SORTED_INDEX_ATTRIBUTE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeBoolean(asc);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        asc = in.readBoolean();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SortedIndexAttribute that = (SortedIndexAttribute) o;

        if (asc != that.asc)
            return false;

        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;

        result = 31 * result + (asc ? 1 : 0);

        return result;
    }

    @Override
    public String toString() {
        return "SortedIndexAttribute{name=" + name + ", asc=" + asc + '}';
    }
}
