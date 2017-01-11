/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.util.Preconditions.checkNotNegative;

/**
 * Configuration options for the {@link com.hazelcast.cardinality.CardinalityEstimator}
 */
public class CardinalityEstimatorConfig {

    /**
     * The number of replicas per estimator
     */
    private static final int DEFAULT_DURABILITY = 1;

    private String name = "default";

    private int durability = DEFAULT_DURABILITY;

    private com.hazelcast.config.CardinalityEstimatorConfig.CardinalityEstimatorConfigReadOnly readOnly;

    public CardinalityEstimatorConfig() {
    }

    public CardinalityEstimatorConfig(String name) {
        this.name = name;
    }

    public CardinalityEstimatorConfig(String name, int durability) {
        this.name = name;
        this.durability = durability;
    }

    public CardinalityEstimatorConfig(com.hazelcast.config.CardinalityEstimatorConfig config) {
        this(config.getName(), config.getDurability());
    }

    public com.hazelcast.config.CardinalityEstimatorConfig.CardinalityEstimatorConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new com.hazelcast.config.CardinalityEstimatorConfig.CardinalityEstimatorConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Gets the name of the cardinality estimator.
     *
     * @return The name of the estimator.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the cardinality estimator.
     *
     * @param name The name of the estimator.
     * @return The cardinality estimator config instance.
     */
    public com.hazelcast.config.CardinalityEstimatorConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the durability of the estimator
     *
     * @return the durability of the estimator
     */
    public int getDurability() {
        return durability;
    }

    /**
     * Sets the durability of the cardinality estimator
     * The durability represents the number of replicas that exist in a cluster for any given estimator.
     * If this is set to 0 then there is only 1 copy of the estimator in the cluster, meaning that if the partition owning it,
     * goes down then the estimation is lost.
     *
     * @param durability the durability of the estimator
     * @return The cardinality estimator config instance.
     */
    public com.hazelcast.config.CardinalityEstimatorConfig setDurability(int durability) {
        checkNotNegative(durability, "durability can't be smaller than 0");
        this.durability = durability;
        return this;
    }

    @Override
    public String toString() {
        return "CardinalityEstimatorConfig{" + "name='" + name + '\'' + ", durability=" + durability + '}';
    }

    private static class CardinalityEstimatorConfigReadOnly
            extends com.hazelcast.config.CardinalityEstimatorConfig {

        CardinalityEstimatorConfigReadOnly(com.hazelcast.config.CardinalityEstimatorConfig config) {
            super(config);
        }

        @Override
        public com.hazelcast.config.CardinalityEstimatorConfig setName(String name) {
            throw new UnsupportedOperationException("This config is read-only cardinality estimator: " + getName());
        }

        @Override
        public com.hazelcast.config.CardinalityEstimatorConfig setDurability(int durability) {
            throw new UnsupportedOperationException("This config is read-only cardinality estimator: " + getName());
        }
    }
}
