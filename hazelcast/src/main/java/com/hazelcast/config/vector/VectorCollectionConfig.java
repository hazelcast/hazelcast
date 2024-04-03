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
package com.hazelcast.config.vector;

import com.hazelcast.config.NamedConfig;
import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.Beta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Configuration object for a vector collection.
 *
 * @since 5.5
 */

@Beta
public class VectorCollectionConfig implements NamedConfig, IdentifiedDataSerializable {

    private String name;
    private final List<VectorIndexConfig> vectorIndexConfigs = new ArrayList<>();

    /**
     * Creates a new, empty {@code VectorCollectionConfig}.
     */
    public VectorCollectionConfig() {
    }

    /**
     * Constructs a VectorCollectionConfig with the given name.
     *
     * @param name the name of the vector collection
     */
    public VectorCollectionConfig(String name) {
        validateName(name);
        this.name = name;
    }

    /**
     * Constructs a new {@code VectorCollectionConfig} instance by copying the values from the provided configuration.
     *
     * @param config The {@link VectorCollectionConfig} instance to copy.
     *               It serves as the source of values for the new configuration.
     */
    public VectorCollectionConfig(VectorCollectionConfig config) {
        requireNonNull(config, "config must not be null.");
        this.name = config.getName();
        setVectorIndexConfigs(config.getVectorIndexConfigs());
    }

    /**
     * Sets the name of the VectorCollection.
     *
     * @param name the name to set for this VectorCollection.
     */
    @Override
    public VectorCollectionConfig setName(String name) {
        validateName(name);
        this.name = name;
        return this;
    }

    /**
     * Returns the name of this VectorCollection
     *
     * @return the name of the VectorCollection
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Adds a vector index configuration to this vector collection configuration.
     *
     * @param vectorIndexConfig the vector index configuration to add
     * @return this VectorCollectionConfig instance
     */
    public VectorCollectionConfig addVectorIndexConfig(VectorIndexConfig vectorIndexConfig) {
        requireNonNull(vectorIndexConfig, "vector index config must not be null.");
        vectorIndexConfigs.add(vectorIndexConfig);
        return this;
    }

    /**
     * Retrieves the list of vector index configurations associated with this vector collection configuration.
     *
     * @return the list of vector index configurations
     */
    public List<VectorIndexConfig> getVectorIndexConfigs() {
        return vectorIndexConfigs;
    }

    /**
     * Sets the list of {@link VectorIndexConfig} instances for this vector collection configuration.
     * Clears the existing vector index configurations and replaces them with the provided list.
     *
     * @param vectorIndexConfigs The list of {@link VectorIndexConfig} instances to set.
     */
    public void setVectorIndexConfigs(List<VectorIndexConfig> vectorIndexConfigs) {
        this.vectorIndexConfigs.clear();
        this.vectorIndexConfigs.addAll(vectorIndexConfigs);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        SerializationUtil.writeList(vectorIndexConfigs, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        vectorIndexConfigs.clear();
        name = in.readString();
        List<VectorIndexConfig> deserialized = SerializationUtil.readList(in);
        vectorIndexConfigs.addAll(deserialized);
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.VECTOR_COLLECTION_CONFIG;
    }

    @Override
    public String toString() {
        return "VectorCollectionConfig{"
                + "name='" + name + '\''
                + ", vectorIndexConfigs=" + vectorIndexConfigs
                + '}';
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        VectorCollectionConfig that = (VectorCollectionConfig) object;
        return Objects.equals(name, that.name) && Objects.equals(vectorIndexConfigs, that.vectorIndexConfigs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, vectorIndexConfigs);
    }

    private static void validateName(String name) {
        String allowedSymbols = "[a-zA-Z0-9\\-*_]+";
        if (!name.matches(allowedSymbols)) {
            throw new IllegalArgumentException("The name of the vector collection "
                    + "should only consist of letters, numbers, and the symbols \"-\", \"_\" or \"*\".");
        }
    }
}
