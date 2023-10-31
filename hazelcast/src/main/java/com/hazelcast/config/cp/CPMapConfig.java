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

import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Configuration for {@link com.hazelcast.cp.CPMap}.
 */
public class CPMapConfig {
    /**
     * The default maximum size in MB that a {@link com.hazelcast.cp.CPMap} can total.
     */
    public static final int DEFAULT_MAX_SIZE_MB = 100;

    /**
     * Maximum size in MB that the key-value pairs of the CPMap can total.
     */
    private int maxSizeMb = DEFAULT_MAX_SIZE_MB;

    /**
     * Name of CPMap.
     */
    private String name;

    public CPMapConfig() {
    }

    public CPMapConfig(String name) {
        this.name = name;
    }

    public CPMapConfig(String name, int maxSizeMb) {
        this.name = name;
        this.maxSizeMb = maxSizeMb;
    }

    public CPMapConfig(CPMapConfig config) {
        this.maxSizeMb = config.maxSizeMb;
    }

    public int getMaxSizeMb() {
        return maxSizeMb;
    }

    public void setMaxSizeMb(int maxSizeMb) {
        checkPositive("maxSizeMb", maxSizeMb);
        this.maxSizeMb = maxSizeMb;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CPMapConfig that = (CPMapConfig) o;
        return maxSizeMb == that.maxSizeMb && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxSizeMb, name);
    }

    @Override
    public String toString() {
        return "CPMapConfig{" + "maxSizeMb=" + maxSizeMb + ", name='" + name + '\'' + '}';
    }
}
