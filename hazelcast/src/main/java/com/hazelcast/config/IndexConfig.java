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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableList;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Configuration of an index. Hazelcast support two types of indexes: sorted index and hash index.
 * Sorted indexes could be used with equality and range predicates and have logarithmic search time.
 * Hash indexes could be used with equality predicates and have constant search time assuming the hash
 * function of the indexed field disperses the elements properly.
 * <p>
 * Index could be created on one or more attributes.
 *
 * @see com.hazelcast.config.IndexType
 * @see IndexAttributeConfig
 * @see com.hazelcast.config.MapConfig#setIndexConfigs(List)
 */
public class IndexConfig implements IdentifiedDataSerializable {
    /** Default index type. */
    public static final IndexType DEFAULT_TYPE = IndexType.SORTED;

    /** Name of the index. */
    private String name;

    /** Type of the index. */
    private IndexType type = DEFAULT_TYPE;

    /** Indexed attributes. */
    private List<IndexAttributeConfig> attributes;

    public IndexConfig() {
        // No-op.
    }

    /**
     * Creates an index configuration of the given type with provided attributes.
     *
     * @param type Index type.
     * @param attributes Attributes to be indexed.
     */
    public IndexConfig(IndexType type, String... attributes) {
        setType(type);

        if (attributes != null) {
            for (String attribute : attributes) {
                addAttribute(attribute);
            }
        }
    }

    public IndexConfig(IndexConfig other) {
        this.name = other.name;
        this.type = other.type;

        for (IndexAttributeConfig attribute : other.getAttributes()) {
            addAttributeInternal(new IndexAttributeConfig(attribute.getName()));
        }
    }

    /**
     * Gets name of the index.
     *
     * @return Name of the index or {@code null} if index name should be generated automatically.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets name of the index.
     *
     * @param name Name of the index or {@code null} if index name should be generated automatically.
     * @return This instance for chaining.
     */
    public IndexConfig setName(String name) {
        this.name = name;

        return this;
    }

    /**
     * Gets type of the index.
     * <p>
     * Defaults to {@link IndexType#SORTED}.
     *
     * @return Type of the index.
     */
    public IndexType getType() {
        return type;
    }

    /**
     * Sets type of the index.
     * <p>
     * Defaults to {@link IndexType#SORTED}.
     *
     * @param type Type of the index.
     * @return This instance for chaining.
     */
    public IndexConfig setType(IndexType type) {
        this.type = checkNotNull(type, "Index type cannot be null.");

        return this;
    }

    /**
     * Gets index attributes.
     *
     * @return Index attributes.
     */
    public List<IndexAttributeConfig> getAttributes() {
        if (attributes == null) {
            attributes = new ArrayList<>();
        }

        return attributes;
    }

    /**
     * Adds an index attribute with the given name and default sort order.
     *
     * @param attribute Attribute name.
     * @return This instance for chaining.
     */
    public IndexConfig addAttribute(String attribute) {
        return addAttribute(new IndexAttributeConfig(attribute));
    }

    /**
     * Adds an index attribute.
     *
     * @param attribute Index attribute.
     * @return This instance for chaining.
     */
    public IndexConfig addAttribute(IndexAttributeConfig attribute) {
        addAttributeInternal(attribute);

        return this;
    }

    protected void addAttributeInternal(IndexAttributeConfig attribute) {
        if (attributes == null) {
            attributes = new ArrayList<>(1);
        }

        attributes.add(attribute);
    }

    /**
     * Sets index attributes.
     *
     * @param attributes Index attributes.
     * @return This instance for chaining.
     */
    public IndexConfig setAttributes(List<IndexAttributeConfig> attributes) {
        if (attributes == null) {
            attributes = Collections.emptyList();
        } else {
            attributes = new ArrayList<>(attributes);
        }

        for (IndexAttributeConfig attribute : attributes) {
            if (attribute == null) {
                throw new NullPointerException("Attribute cannot be null.");
            }
        }

        this.attributes = attributes;

        return this;
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public IndexConfig getAsReadOnly() {
        return new IndexConfigReadOnly(this);
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.INDEX_CONFIG;
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
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexConfig that = (IndexConfig) o;

        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }

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
        return "IndexConfig{name=" + name + ", type=" + type + ", attributes=" + getAttributes() + '}';
    }
}
