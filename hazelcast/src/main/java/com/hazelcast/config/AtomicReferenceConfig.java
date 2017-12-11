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
 * Contains the configuration for an {@link java.util.concurrent.atomic.AtomicReference}.
 */
public class AtomicReferenceConfig extends AbstractBasicConfig<AtomicReferenceConfig> {

    private transient AtomicReferenceConfigReadOnly readOnly;

    public AtomicReferenceConfig() {
    }

    public AtomicReferenceConfig(String name) {
        setName(name);
    }

    public AtomicReferenceConfig(AtomicReferenceConfig config) {
        super(config);
    }

    @Override
    public String toString() {
        return "AtomicReferenceConfig{"
                + fieldsToString()
                + "}";
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.ATOMIC_REFERENCE_CONFIG;
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    @Override
    public AtomicReferenceConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new AtomicReferenceConfigReadOnly(this);
        }
        return readOnly;
    }

    static class AtomicReferenceConfigReadOnly extends AtomicReferenceConfig {

        AtomicReferenceConfigReadOnly(AtomicReferenceConfig config) {
            super(config);
        }

        @Override
        public AtomicReferenceConfig setName(String name) {
            throw new UnsupportedOperationException("This is a read-only config!");
        }
    }
}
