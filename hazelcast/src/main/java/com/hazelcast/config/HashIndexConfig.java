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
 * Configuration of a hash index.
 */
public class HashIndexConfig extends IndexConfig {
    /** List of attributes to be indexed. */
    private List<String> attributes;

    public HashIndexConfig() {
        // No-op.
    }

    /**
     * Create a hash index over a single attribute.
     *
     * @param attribute Name of the attribute.
     */
    public HashIndexConfig(String attribute) {
        addAttribute(attribute);
    }

    /**
     * Sets name of the index.
     *
     * @param name Name of the index or {@code null} if index name should be generated automatically.
     * @return This instance for chaining.
     */
    public HashIndexConfig setName(String name) {
        this.name = name;

        return this;
    }

    /**
     * Gets the list of attributes to be indexed.
     *
     * @return Attribute names.
     */
    public List<String> getAttributes() {
        if (attributes == null)
            attributes = new ArrayList<>();

        return attributes;
    }

    /**
     * Adds an attribute to be indexed.
     *
     * @param attribute Attribute name.
     * @return This instance for chaining.
     */
    public HashIndexConfig addAttribute(String attribute) {
        getAttributes().add(attribute);

        return this;
    }

    /**
     * Sets the list of attributes to be indexed.
     *
     * @param attributes Attribute names.
     * @return This instance for chaining.
     */
    public HashIndexConfig setAttributes(List<String> attributes) {
        this.attributes = attributes;


        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.HASH_INDEX_CONFIG;
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

        HashIndexConfig that = (HashIndexConfig)o;

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
        return "HashIndexConfig{name=" + name + ", attributes=" + attributes + '}';
    }
}
