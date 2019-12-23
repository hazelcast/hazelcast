/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.QueryConstants;
import com.hazelcast.query.impl.IndexUtils;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableList;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Configuration of an index. Hazelcast support two types of indexes: sorted index and hash index.
 * Sorted indexes could be used with equality and range predicates and have logarithmic search time.
 * Hash indexes could be used with equality predicates and have constant search time assuming the hash
 * function of the indexed field disperses the elements properly.
 * <p>
 * Index could be created on one or more attributes.
 *
 * @see com.hazelcast.config.IndexType
 * @see com.hazelcast.config.MapConfig#setIndexConfigs(List)
 */
public class IndexConfig implements IdentifiedDataSerializable {
    /** Default index type. */
    public static final IndexType DEFAULT_TYPE = IndexType.SORTED;

    /** Default unique key. */
    public static final String DEFAULT_UNIQUE_KEY = QueryConstants.KEY_ATTRIBUTE_NAME.value();
    /** Default unique key transform. */
    public static final UniqueKeyTransform DEFAULT_UNIQUE_KEY_TRANSFORM = UniqueKeyTransform.OBJECT;

    /** Name of the index. */
    private String name;

    /** Type of the index. */
    private IndexType type = DEFAULT_TYPE;

    /** Bitmap index options: unique key attribute path and its transform. */
    private String uniqueKey = DEFAULT_UNIQUE_KEY;
    private UniqueKeyTransform uniqueKeyTransform = DEFAULT_UNIQUE_KEY_TRANSFORM;

    /** Indexed attributes. */
    private List<String> attributes;

    public IndexConfig() {
        // No-op.
    }

    /**
     * Creates an index configuration of the given type.
     *
     * @param type Index type.
     */
    public IndexConfig(IndexType type) {
        setType(type);
    }

    /**
     * Creates an index configuration of the given type with provided attributes.
     *
     * @param type Index type.
     * @param attributes Attributes to be indexed.
     */
    public IndexConfig(IndexType type, String... attributes) {
        this(type);

        if (attributes != null) {
            for (String attribute : attributes) {
                addAttribute(attribute);
            }
        }
    }

    public IndexConfig(IndexConfig other) {
        this.name = other.name;
        this.type = other.type;
        this.uniqueKey = other.uniqueKey;
        this.uniqueKeyTransform = other.uniqueKeyTransform;

        for (String attribute : other.getAttributes()) {
            addAttributeInternal(attribute);
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
     * Returns the unique key attribute configured in this index config.
     * Defaults to {@code __key}. The unique key attribute is used as a source
     * of values which uniquely identify each entry being inserted into an index.
     * <p>
     * Currently, applicable only to bitmap indexes.
     *
     * @return the configured unique key attribute.
     */
    public String getUniqueKey() {
        return uniqueKey;
    }

    /**
     * Sets unique key attribute in this index config.
     * <p>
     * Currently, applicable only to bitmap indexes.
     *
     * @param uniqueKey a unique key attribute to configure.
     */
    public IndexConfig setUniqueKey(@Nonnull String uniqueKey) {
        checkNotNull(uniqueKey, "unique key can't be null");
        this.uniqueKey = uniqueKey;
        return this;
    }

    /**
     * Returns the unique key transform configured in this index. Defaults to
     * {@link UniqueKeyTransform#OBJECT OBJECT}. The transform is applied to
     * every value extracted from {@link #getUniqueKey() unique key attribue}.
     * <p>
     * Currently, applicable only to bitmap indexes.
     *
     * @return the configured unique key transform.
     */
    public UniqueKeyTransform getUniqueKeyTransform() {
        return uniqueKeyTransform;
    }

    /**
     * Sets unique key transform in this index config.
     * <p>
     * Currently, applicable only to bitmap indexes.
     *
     * @param uniqueKeyTransform a unique key transform to configure.
     */
    public IndexConfig setUniqueKeyTransform(@Nonnull UniqueKeyTransform uniqueKeyTransform) {
        checkNotNull(uniqueKeyTransform, "unique key transform can't be null");
        this.uniqueKeyTransform = uniqueKeyTransform;
        return this;
    }

    /**
     * Gets index attributes.
     *
     * @return Index attributes.
     */
    public List<String> getAttributes() {
        if (attributes == null) {
            attributes = new ArrayList<>();
        }

        return attributes;
    }

    /**
     * Adds an index attribute with the given.
     *
     * @param attribute Attribute name.
     * @return This instance for chaining.
     */
    public IndexConfig addAttribute(String attribute) {
        addAttributeInternal(attribute);

        return this;
    }

    public void addAttributeInternal(String attribute) {
        IndexUtils.validateAttribute(attribute);

        if (attributes == null) {
            attributes = new ArrayList<>();
        }

        attributes.add(attribute);
    }

    /**
     * Sets index attributes.
     *
     * @param attributes Index attributes.
     * @return This instance for chaining.
     */
    public IndexConfig setAttributes(List<String> attributes) {
        checkNotNull(attributes, "Index attributes cannot be null.");

        this.attributes = new ArrayList<>(attributes.size());

        for (String attribute : attributes) {
            addAttribute(attribute);
        }

        return this;
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
        out.writeInt(type.getId());
        out.writeUTF(uniqueKey);
        out.writeInt(uniqueKeyTransform.getId());
        writeNullableList(attributes, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        type = IndexType.getById(in.readInt());
        uniqueKey = in.readUTF();
        uniqueKeyTransform = UniqueKeyTransform.fromId(in.readInt());
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

        if (!Objects.equals(name, that.name)) {
            return false;
        }

        if (!Objects.equals(type, that.type)) {
            return false;
        }

        if (!Objects.equals(uniqueKey, that.uniqueKey)) {
            return false;
        }

        if (uniqueKeyTransform != that.uniqueKeyTransform) {
            return false;
        }

        return getAttributes().equals(that.getAttributes());
    }

    @Override
    public int hashCode() {
        int result = (name != null ? name.hashCode() : 0);

        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + uniqueKey.hashCode();
        result = 31 * result + uniqueKeyTransform.hashCode();
        result = 31 * result + getAttributes().hashCode();

        return result;
    }

    @Override
    public String toString() {
        return "IndexConfig{name=" + name + ", type=" + type + ", uniqueKey=" + uniqueKey + ", uniqueKeyTransform="
                + uniqueKeyTransform + ", attributes=" + getAttributes() + '}';
    }
}
