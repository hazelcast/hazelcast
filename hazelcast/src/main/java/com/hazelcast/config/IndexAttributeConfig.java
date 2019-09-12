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
import com.hazelcast.query.impl.IndexUtils;

import java.io.IOException;

/**
 * An attribute to be indexed.
 */
public class IndexAttributeConfig implements IdentifiedDataSerializable {
    /** Default sort order of the attribute. */
    public static final boolean DEFAULT_ASC = true;

    /** Name of the attribute. */
    private String name;

    /** Whether the attribute is sorted in ascending order. */
    protected boolean asc = DEFAULT_ASC;

    public IndexAttributeConfig() {
        // No-op.
    }

    /**
     * Creates an index attribute with the given name sorted in ascending order.
     *
     * @param name Name of the attribute.
     */
    public IndexAttributeConfig(String name) {
        setName(name);
    }

    public IndexAttributeConfig(IndexAttributeConfig other) {
        this.name = other.name;
        this.asc = other.asc;
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
    public IndexAttributeConfig setName(String name) {
        IndexUtils.validateAttribute(name);

        this.name = name;

        return this;
    }

    /**
     * Gets whether the attribute should be indexed in ascending order. Applicable only to {@link IndexType#SORTED}
     * index, ignored by other index types.
     * <p>
     * Only ascending order is supported at the moment.
     *
     * @return {@code True} if the attribute should be indexed in ascending order, {@code false} otherwise.
     */
    public boolean isAscending() {
        return asc;
    }

    /**
     * Sets whether the attribute should be indexed in ascending order. Applicable only to {@link IndexType#SORTED}
     * index, ignored by other index types.
     * <p>
     * Only ascending order is supported at the moment.
     *
     * @param asc {@code True} if the attribute should be indexed in ascending order, {@code false} otherwise.
     * @return This instance for chaining.
     */
    @SuppressWarnings("ConstantConditions")
    public IndexAttributeConfig setAscending(boolean asc) {
        if (!asc) {
            throw new IllegalArgumentException("Only ascending order is supported at the moment.");
        }

        this.asc = asc;

        return this;
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public IndexAttributeConfig getAsReadOnly() {
        return new IndexAttributeConfigReadOnly(this);
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.INDEX_ATTRIBUTE;
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
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexAttributeConfig that = (IndexAttributeConfig) o;

        if (asc != that.asc) {
            return false;
        }

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
        return "IndexAttributeConfig{name=" + name + ", asc=" + asc + '}';
    }
}
