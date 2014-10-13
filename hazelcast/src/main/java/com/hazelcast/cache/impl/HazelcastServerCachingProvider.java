/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;

import java.net.URI;
import java.util.Properties;

/**
 * Provides server cachingProvider implementation.
 * <p>This implementation is used by {@link com.hazelcast.cache.impl.HazelcastCachingProvider} internally when
 * server type is configured.</p>
 * <p>This implementation creates a new singleton hazelcastInstance node. This instance is provided into the
 * created managers.</p>
 * <p>If you need to use your already created HazelcastInstance, you can directly create a provider using
 * {@link #createCachingProvider(com.hazelcast.core.HazelcastInstance)}.</p>
 * @see javax.cache.spi.CachingProvider
 */
public final class HazelcastServerCachingProvider
        extends HazelcastAbstractCachingProvider {

    // unnecessary
    public HazelcastServerCachingProvider() {
        super();
    }

    /**
     * Helper method for creating caching provider for testing, etc.
     *
     * @param hazelcastInstance
     * @return HazelcastServerCachingProvider
     */
    public static HazelcastServerCachingProvider createCachingProvider(HazelcastInstance hazelcastInstance) {
        final HazelcastServerCachingProvider cachingProvider = new HazelcastServerCachingProvider();
        cachingProvider.hazelcastInstance = hazelcastInstance;
        return cachingProvider;
    }

    private HazelcastInstance initHazelcast() {
        Config config = new XmlConfigBuilder().build();
        if (config.getInstanceName() == null) {
            config.setInstanceName("CacheProvider");
        }
        return HazelcastInstanceFactory.getOrCreateHazelcastInstance(config);
    }

    @Override
    protected HazelcastServerCacheManager createHazelcastCacheManager(URI uri, ClassLoader classLoader,
                                                                      Properties managerProperties) {
        if (hazelcastInstance == null) {
            //initHazelcast always return a singleton instance
            hazelcastInstance = initHazelcast();
        }
        return new HazelcastServerCacheManager(this, hazelcastInstance, uri, classLoader, managerProperties);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HazelcastServerCachingProvider{");
        sb.append("hazelcastInstance=").append(hazelcastInstance);
        sb.append('}');
        return sb.toString();
    }
}
