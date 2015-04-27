/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
 * Configuration for cache eviction.
 *
 * @see com.hazelcast.config.EvictionConfig
 *
 * @deprecated Use {@link com.hazelcast.config.EvictionConfig} instead of this
 */
@Deprecated
public class CacheEvictionConfig
        extends EvictionConfig {

    public CacheEvictionConfig() {
    }

    public CacheEvictionConfig(int size, MaxSizePolicy maxSizePolicy, EvictionPolicy evictionPolicy) {
        super(size, maxSizePolicy, evictionPolicy);
    }

    public CacheEvictionConfig(EvictionConfig config) {
        super(config);
    }

    @Override
    public EvictionConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new CacheEvictionConfigReadOnly(this);
        }
        return readOnly;
    }

    @Override
    public String toString() {
        return "CacheEvictionConfig{"
                + "size=" + size
                + ", maxSizePolicy=" + maxSizePolicy
                + ", evictionPolicy=" + evictionPolicy
                + ", readOnly=" + readOnly
                + '}';
    }

}
