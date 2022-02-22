/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.internal.config.ConfigDataSerializerHook;

import java.io.Serializable;

/**
 * Configuration for CachePartitionLostListener
 *
 * @see CachePartitionLostListener
 */
public class CachePartitionLostListenerConfig extends ListenerConfig implements Serializable {

    public CachePartitionLostListenerConfig() {
    }

    public CachePartitionLostListenerConfig(String className) {
        super(className);
    }

    public CachePartitionLostListenerConfig(CachePartitionLostListener implementation) {
        super(implementation);
    }

    public CachePartitionLostListenerConfig(CachePartitionLostListenerConfig config) {
        implementation = config.getImplementation();
        className = config.getClassName();
    }

    @Override
    public CachePartitionLostListener getImplementation() {
        return (CachePartitionLostListener) implementation;
    }

    public CachePartitionLostListenerConfig setImplementation(final CachePartitionLostListener implementation) {
        super.setImplementation(implementation);
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
        if (!super.equals(o)) {
            return false;
        }

        CachePartitionLostListenerConfig that = (CachePartitionLostListenerConfig) o;
        return !(implementation != null ? !implementation.equals(that.implementation) : that.implementation != null);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (className != null ? className.hashCode() : 0);
        result = 31 * result + (implementation != null ? implementation.hashCode() : 0);
        return result;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.CACHE_PARTITION_LOST_LISTENER_CONFIG;
    }
}
