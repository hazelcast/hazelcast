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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableList;

/**
 * Configuration of a sorted index.
 *
 * @see SortedIndexAttribute
 * @see MapConfig#setIndexConfigs(List)
 */
public class SortedIndexConfig extends IndexConfig {
    /** List of attributes to be indexed. */
    private List<SortedIndexAttribute> attributes;

    /**
     * Creates an empty index config.
     */
    public SortedIndexConfig() {
        // No-op.
    }

    /**
     * Creates an index config on a single attribute with ascending sort order.
     *
     * @param attributeName Name of the attribute.
     */
    public SortedIndexConfig(String attributeName) {
        this(attributeName, SortedIndexAttribute.DEFAULT_ASC);
    }

    /**
     * Creates an index config on a single attribute.
     *
     * @param attributeName Name of the attribute.
     * @param asc {@code True} if the attribute should be sorted in ascending order, {@code false} otherwise.
     */
    public SortedIndexConfig(String attributeName, boolean asc) {
        addAttribute(new SortedIndexAttribute(attributeName, asc));
    }

    /**
     * Sets name of the index.
     *
     * @param name Name of the index or {@code null} if index name should be generated automatically.
     * @return This instance for chaining.
     */
    public SortedIndexConfig setName(String name) {
        this.name = name;

        return this;
    }

    /**
     * Gets index attributes.
     *
     * @return Index attributes.
     */
    public List<SortedIndexAttribute> getAttributes() {
        if (attributes == null)
            attributes = new ArrayList<>();

        return attributes;
    }

    /**
     * Adds an index attribute.
     *
     * @param attribute Index attribute.
     * @return This instance for chaining.
     */
    public SortedIndexConfig addAttribute(SortedIndexAttribute attribute) {
        getAttributes().add(attribute);

        return this;
    }

    /**
     * Sets index attributes.
     *
     * @param attributes Index attributes.
     * @return This instance for chaining.
     */
    public SortedIndexConfig setAttributes(List<SortedIndexAttribute> attributes) {
        this.attributes = attributes;

        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.SORTED_INDEX_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        writeNullableList(attributes, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        attributes = readNullableList(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SortedIndexConfig that = (SortedIndexConfig) o;

        if (name != null ? name.equals(that.name) : that.name == null)
            return false;

        return getAttributes().equals(that.getAttributes());
    }

    @Override
    public int hashCode() {
        int result = (name != null ? name.hashCode() : 0);

        result = 31 * result + getAttributes().hashCode();

        return result;
    }

    @Override
    public String toString() {
        return "SortedIndexConfig{name=" + name + ", attributes=" + attributes + '}';
    }
}
