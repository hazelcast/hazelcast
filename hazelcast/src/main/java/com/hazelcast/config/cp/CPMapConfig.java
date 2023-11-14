/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.cp;

import javax.annotation.Nullable;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Configuration for {@link com.hazelcast.cp.CPMap}.
 * @since 5.4
 */
public class CPMapConfig {
    /**
     * The default maximum size in MB that a {@link com.hazelcast.cp.CPMap} can total.
     */
    public static final int DEFAULT_MAX_SIZE_MB = 100;
    // this mirrors the current limitation on the in-memory CPMapStore -- it's intentionally not exposed
    private static final int LIMIT_MAX_SIZE_MB = 2_000;

    /**
     * Name of the CPMap.
     */
    private String name;

    /**
     * Maximum size in MB that the key-value pairs of the CPMap can total.
     */
    private int maxSizeMb = DEFAULT_MAX_SIZE_MB;

    /**
     * @deprecated exists only for DOM processing.
     */
    public CPMapConfig() {
    }

    /**
     * Creates a configuration for a {@link com.hazelcast.cp.CPMap} with the given {@code name} and the default map capacity of
     * {@link CPMapConfig#DEFAULT_MAX_SIZE_MB}.
     * @param name of the {@link com.hazelcast.cp.CPMap} the configuration is applicable for
     */
    public CPMapConfig(String name) {
        this(name, DEFAULT_MAX_SIZE_MB);
    }

    /**
     * Creates a configuration for a {@link com.hazelcast.cp.CPMap} with the given {@code name} and maximum capacity in MB as
     * specified by {@code maxSizeMb}.
     * {@link CPMapConfig#DEFAULT_MAX_SIZE_MB}.
     * @param name of the {@link com.hazelcast.cp.CPMap} the configuration is applicable for
     * @param maxSizeMb maximum MB capacity of the {@link com.hazelcast.cp.CPMap}
     */
    public CPMapConfig(String name, int maxSizeMb) {
        setName(name).setMaxSizeMb(maxSizeMb);
    }

    /**
     * Copy constructor.
     * @param config to copy
     */
    public CPMapConfig(CPMapConfig config) {
        setName(config.name).setMaxSizeMb(config.maxSizeMb);
    }

    /**
     * Gets the maximum capacity in MB.
     */
    public int getMaxSizeMb() {
        return maxSizeMb;
    }

    /**
     * Sets the maximum capacity of the {@link com.hazelcast.cp.CPMap}.
     * @param maxSizeMb capacity of the {@link com.hazelcast.cp.CPMap} in MB
     * @throws IllegalArgumentException if {@code maxSizeMb} is not positive or exceeds 2000MB
     */
    public CPMapConfig setMaxSizeMb(int maxSizeMb) {
        checkPositive("maxSizeMb", maxSizeMb);
        if (maxSizeMb > LIMIT_MAX_SIZE_MB) {
            throw new IllegalArgumentException("maxSizeMb is " + maxSizeMb + " but must be <= " + LIMIT_MAX_SIZE_MB);
        }
        this.maxSizeMb = maxSizeMb;
        return this;
    }

    /**
     * Gets the name of the configuration.
     */
    @Nullable
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the configuration.
     * @throws NullPointerException if the {@code name} is null
     */
    public CPMapConfig setName(String name) {
        this.name = checkNotNull(name, "Name must not be null");
        return this;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        CPMapConfig that = (CPMapConfig) object;
        return maxSizeMb == that.maxSizeMb && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, maxSizeMb);
    }

    @Override
    public String toString() {
        return "CPMapConfig{" + "name='" + name + '\'' + ", maxSizeMb=" + maxSizeMb + '}';
    }
}
