/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
 * Created by Thomas Kountis.
 */
public class ScheduledExecutorConfig {

    /**
     * Scheduled of Executor
     */
    public static final int DEFAULT_DURABILITY = 1;

    private String name = "default";

    private int durability = DEFAULT_DURABILITY;

    private com.hazelcast.config.ScheduledExecutorConfig.ScheduledExecutorConfigReadOnly readOnly;

    public ScheduledExecutorConfig() {
    }

    public ScheduledExecutorConfig(String name) {
        this.name = name;
    }

    public ScheduledExecutorConfig(String name, int durability) {
        this.name = name;
        this.durability = durability;
    }

    public ScheduledExecutorConfig(com.hazelcast.config.ScheduledExecutorConfig config) {
        this(config.getName(), config.getDurability());
    }

    public com.hazelcast.config.ScheduledExecutorConfig.ScheduledExecutorConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new com.hazelcast.config.ScheduledExecutorConfig.ScheduledExecutorConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Gets the name of the executor task.
     *
     * @return The name of the executor task.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the executor task.
     *
     * @param name The name of the executor task.
     * @return This executor config instance.
     */
    public com.hazelcast.config.ScheduledExecutorConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the durability of the executor
     *
     * @return the durability of the executor
     */
    public int getDurability() {
        return durability;
    }

    /**
     * Sets the durability of the executor
     *
     * @param durability the durability of the executor
     * @return This executor config instance.
     */
    public com.hazelcast.config.ScheduledExecutorConfig setDurability(int durability) {
        checkNotNegative(durability, "durability can't be smaller than 0");
        this.durability = durability;
        return this;
    }

    @Override
    public String toString() {
        return "ScheduledExecutorConfig{"
                + "name='" + name + '\''
                + ", durability=" + durability
                + '}';
    }

    private static class ScheduledExecutorConfigReadOnly extends com.hazelcast.config.ScheduledExecutorConfig {

        public ScheduledExecutorConfigReadOnly(com.hazelcast.config.ScheduledExecutorConfig config) {
            super(config);
        }

        @Override
        public com.hazelcast.config.ScheduledExecutorConfig setName(String name) {
            throw new UnsupportedOperationException("This config is read-only scheduled executor: " + getName());
        }

        @Override
        public com.hazelcast.config.ScheduledExecutorConfig setDurability(int durability) {
            throw new UnsupportedOperationException("This config is read-only scheduled executor: " + getName());
        }
    }

}
