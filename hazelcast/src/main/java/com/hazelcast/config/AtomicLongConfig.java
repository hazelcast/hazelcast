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

/**
 * Contains the configuration for an {@link java.util.concurrent.atomic.AtomicLong}.
 */
public class AtomicLongConfig extends AbstractBasicConfig<AtomicLongConfig> {

    private transient AtomicLongConfigReadOnly readOnly;

    public AtomicLongConfig() {
    }

    public AtomicLongConfig(String name) {
        setName(name);
    }

    public AtomicLongConfig(AtomicLongConfig config) {
        super(config);
    }

    @Override
    public String toString() {
        return "AtomicLongConfig{"
                + fieldsToString()
                + "}";
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.ATOMIC_LONG_CONFIG;
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    @Override
    public AtomicLongConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new AtomicLongConfigReadOnly(this);
        }
        return readOnly;
    }

    static class AtomicLongConfigReadOnly extends AtomicLongConfig {

        AtomicLongConfigReadOnly(AtomicLongConfig config) {
            super(config);
        }

        @Override
        public AtomicLongConfig setName(String name) {
            throw new UnsupportedOperationException("This is a read-only config!");
        }
    }
}
