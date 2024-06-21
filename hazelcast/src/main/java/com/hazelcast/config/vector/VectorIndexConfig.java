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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.Beta;

import java.io.IOException;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Configuration for a vector index.
 */

@Beta
public class VectorIndexConfig implements NamedConfig, IdentifiedDataSerializable {

    private static final int DEFAULT_MAX_DEGREE = 16;
    private static final int DEFAULT_EF_CONSTRUCTION = 100;
    private static final boolean DEFAULT_USE_DEDUPLICATION = true;

    /**
     * Name of index. Names of indexes within a single VectorCollection
     * must be unique and non-empty. Valid characters are {@code [a-zA-Z0-9_-]}.
     */
    private String indexName;
    private Metric metric;
    private int dimension;
    private int maxDegree = DEFAULT_MAX_DEGREE;
    private int efConstruction = DEFAULT_EF_CONSTRUCTION;
    private boolean useDeduplication = DEFAULT_USE_DEDUPLICATION;

    /**
     * Constructs a VectorIndexConfig with the given parameters.
     *
     * @param indexName the name of the index
     * @param metric    the distance metric of the index
     * @param dimension the dimension of the index
     * @throws IllegalArgumentException if the indexName is invalid.
     * @throws NullPointerException     if the metric is null.
     */
    public VectorIndexConfig(String indexName, Metric metric, int dimension) {
        validateName(indexName);
        requireNonNull(metric, "metric must not be null.");
        this.indexName = indexName;
        this.metric = metric;
        this.dimension = dimension;
    }

    /**
     * Constructs a VectorIndexConfig object with the specified parameters.
     *
     * @param indexName        The name of the index.
     * @param metric           The metric used for indexing.
     * @param dimension        The dimension of the vectors to be indexed.
     * @param maxDegree        The maximum number of connections allowed per node.
     * @param efConstruction        The size of the dynamic list for search.
     * @param useDeduplication Whether to enable deduplication in the index.
     * @throws IllegalArgumentException if the indexName is invalid.
     * @throws NullPointerException     if the metric is null.
     */
    public VectorIndexConfig(
            String indexName,
            Metric metric,
            int dimension,
            int maxDegree,
            int efConstruction,
            boolean useDeduplication
    ) {
        validateName(indexName);
        requireNonNull(metric, "metric must not be null.");
        if (maxDegree <= 0) {
            throw new IllegalArgumentException("max connections must be positive");
        }
        if (efConstruction <= 0) {
            throw new IllegalArgumentException("efConstruction must be positive");
        }
        this.indexName = indexName;
        this.metric = metric;
        this.dimension = dimension;
        this.maxDegree = maxDegree;
        this.efConstruction = efConstruction;
        this.useDeduplication = useDeduplication;
    }

    /**
     * Constructs an empty VectorIndexConfig.
     */
    public VectorIndexConfig() {
    }

    /**
     * Constructs a new {@code VectorIndexConfig} instance by copying the values from the provided configuration.
     *
     * @param config The {@link VectorIndexConfig} instance to copy.
     *               It serves as the source of values for the new configuration.
     */

    public VectorIndexConfig(VectorIndexConfig config) {
        requireNonNull(config, "config must not be null.");
        this.indexName = config.getName();
        this.metric = config.getMetric();
        this.dimension = config.getDimension();
        this.maxDegree = config.getMaxDegree();
        this.efConstruction = config.getEfConstruction();
        this.useDeduplication = config.isUseDeduplication();
    }

    /**
     * Retrieves the metric of this vector index configuration.
     *
     * @return the metric of the index
     */
    public Metric getMetric() {
        return metric;
    }

    /**
     * Sets the metric of this vector index configuration.
     *
     * @param metric the metric to set
     * @return this VectorIndexConfig instance
     */
    public VectorIndexConfig setMetric(Metric metric) {
        requireNonNull(metric, "metric must not be null.");
        this.metric = metric;
        return this;
    }

    /**
     * Retrieves the dimension of this vector index configuration.
     *
     * @return the dimension of the index
     */
    public int getDimension() {
        return dimension;
    }


    /**
     * Sets the dimension of this vector index configuration.
     *
     * @param dimension the dimension to set
     * @return this VectorIndexConfig instance
     */
    public VectorIndexConfig setDimension(int dimension) {
        this.dimension = dimension;
        return this;
    }

    /**
     * Retrieves the maximum number of connections a node can have.
     *
     * @return The maximum number of connections allowed per node.
     */
    public int getMaxDegree() {
        return maxDegree;
    }

    /**
     * Sets the maximum number of connections allowed per node.
     *
     * @param maxDegree The maximum degree to set.
     * @return this VectorIndexConfig instance
     */
    public VectorIndexConfig setMaxDegree(int maxDegree) {
        if (maxDegree <= 0) {
            throw new IllegalArgumentException("max connections must be positive");
        }
        this.maxDegree = maxDegree;
        return this;
    }

    /**
     * Retrieves the size of the dynamic list search to use during index time.
     *
     * @return the size of the dynamic list search.
     */
    public int getEfConstruction() {
        return efConstruction;
    }

    /**
     * Sets the size of the dynamic list search to use during index time.
     *
     * @param efConstruction the size of the dynamic list search.
     * @return this VectorIndexConfig instance
     */
    public VectorIndexConfig setEfConstruction(int efConstruction) {
        if (efConstruction <= 0) {
            throw new IllegalArgumentException("efConstruction width must be positive");
        }
        this.efConstruction = efConstruction;
        return this;
    }

    /**
     * Checks if deduplication is enabled.
     *
     * @return True if deduplication is enabled, false otherwise.
     */
    public boolean isUseDeduplication() {
        return useDeduplication;
    }

    /**
     * Enables or disables deduplication.
     *
     * @param useDeduplication True to enable deduplication, false to disable.
     * @return this VectorIndexConfig instance
     */
    public VectorIndexConfig setUseDeduplication(boolean useDeduplication) {
        this.useDeduplication = useDeduplication;
        return this;
    }


    /**
     * Sets the name of the vector index.
     *
     * @param name the name to set for this vector index.
     */
    @Override
    public VectorIndexConfig setName(String name) {
        validateName(name);
        this.indexName = name;
        return this;
    }

    /**
     * Returns the name of this vector index
     *
     * @return the name of the vector index
     */
    @Override
    public String getName() {
        return indexName;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(indexName);
        out.writeInt(dimension);
        out.writeInt(metric.getId());
        out.writeInt(maxDegree);
        out.writeInt(efConstruction);
        out.writeBoolean(useDeduplication);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        indexName = in.readString();
        dimension = in.readInt();
        metric = Metric.getById(in.readInt());
        maxDegree = in.readInt();
        efConstruction = in.readInt();
        useDeduplication = in.readBoolean();
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.VECTOR_INDEX_CONFIG;
    }

    private static void validateName(String name) {
        if (name == null) {
            return;
        }
        String allowedSymbols = "[a-zA-Z0-9\\-_]+";
        if (!name.matches(allowedSymbols)) {
            throw new IllegalArgumentException("The name of the vector index "
                    + "should only consist of letters, numbers, and the symbols \"-\" or \"_\".");
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        VectorIndexConfig that = (VectorIndexConfig) object;
        return dimension == that.dimension
                && maxDegree == that.maxDegree
                && efConstruction == that.efConstruction
                && useDeduplication == that.useDeduplication
                && Objects.equals(indexName, that.indexName)
                && metric == that.metric;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, metric, dimension, maxDegree, efConstruction, useDeduplication);
    }

    @Override
    public String toString() {
        return "VectorIndexConfig{"
                + "indexName='" + indexName + '\''
                + ", metric=" + metric
                + ", dimension=" + dimension
                + ", maxDegree=" + maxDegree
                + ", efConstruction=" + efConstruction
                + ", useDeduplication=" + useDeduplication
                + '}';
    }
}
