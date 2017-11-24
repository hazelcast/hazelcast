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

package com.hazelcast.client.config;

/**
 * The {@code FlakeIdGeneratorConfig} contains the configuration for the client
 * regarding {@link com.hazelcast.core.HazelcastInstance#getFlakeIdGenerator(String)
 * Flake ID Generator}.
 */
public class FlakeIdGeneratorConfig {

    /**
     * Default value for {@link #getPrefetchCount()}.
     */
    public static final int DEFAULT_PREFETCH_COUNT = 100;

    /**
     * Default value for {@link #getPrefetchValidity()}.
     */
    public static final long DEFAULT_PREFETCH_VALIDITY = 10000;

    private String name;
    private int prefetchCount = DEFAULT_PREFETCH_COUNT;
    private long prefetchValidity = DEFAULT_PREFETCH_VALIDITY;

    // for spring-instantiation
    @SuppressWarnings("unused")
    private FlakeIdGeneratorConfig() {
    }

    public FlakeIdGeneratorConfig(String name) {
        this.name = name;
    }

    /**
     * Copy-constructor
     */
    public FlakeIdGeneratorConfig(FlakeIdGeneratorConfig other) {
        this.name = other.name;
        this.prefetchCount = other.prefetchCount;
        this.prefetchValidity = other.prefetchValidity;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPrefetchCount() {
        return Math.max(1, prefetchCount);
    }

    /**
     * How many IDs are pre-fetched on the background when one call to
     * {@link com.hazelcast.core.FlakeIdGenerator#newId()} is made.
     * <p>
     * Value must be >= 1, default is 100.
     *
     * @return this instance for fluent API
     */
    public FlakeIdGeneratorConfig setPrefetchCount(int prefetchCount) {
        this.prefetchCount = prefetchCount;
        return this;
    }

    public long getPrefetchValidity() {
        return prefetchValidity;
    }

    /**
     * For how long the pre-fetched IDs can be used. If this time elapses, new IDs will be fetched.
     * <p>
     * Time unit is milliseconds.
     * <p>
     * If value is &lt;= 0, validity is unlimited. Default value is 10000 (10 seconds).
     * <p>
     * The IDs contain timestamp component, which ensures rough global ordering of IDs. If an ID
     * is assigned to an event that occurred much later, it will be much out of order. If you don't need
     * ordering, set this value to 0.
     *
     * @return this instance for fluent API
     */
    public FlakeIdGeneratorConfig setPrefetchValidity(long prefetchValidity) {
        this.prefetchValidity = prefetchValidity;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FlakeIdGeneratorConfig that = (FlakeIdGeneratorConfig) o;

        if (prefetchCount != that.prefetchCount) {
            return false;
        }
        if (prefetchValidity != that.prefetchValidity) {
            return false;
        }
        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + prefetchCount;
        result = 31 * result + (int) (prefetchValidity ^ (prefetchValidity >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "FlakeIdGeneratorConfig{"
                + "name='" + name + '\''
                + ", prefetchCount=" + prefetchCount
                + ", prefetchValidity=" + prefetchValidity
                + '}';
    }
}
