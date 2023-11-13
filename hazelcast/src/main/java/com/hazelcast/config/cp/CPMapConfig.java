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

    /**
     * Name of CPMap.
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

    public CPMapConfig(String name) {
        this(name, DEFAULT_MAX_SIZE_MB);
    }

    public CPMapConfig(String name, int maxSizeMb) {
        setName(name).setMaxSizeMb(maxSizeMb);
    }

    public CPMapConfig(CPMapConfig config) {
        setName(config.name).setMaxSizeMb(config.maxSizeMb);
    }

    public int getMaxSizeMb() {
        return maxSizeMb;
    }

    public CPMapConfig setMaxSizeMb(int maxSizeMb) {
        checkPositive("maxSizeMb", maxSizeMb);
        this.maxSizeMb = maxSizeMb;
        return this;
    }

    @Nullable
    public String getName() {
        return name;
    }

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
