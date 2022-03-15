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

package com.hazelcast.spring.cache;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import javax.cache.CacheManager;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Spring utility class for connecting {@link HazelcastCachingProvider} interface and Hazelcast instance.
 */
public final class SpringHazelcastCachingProvider {

    private SpringHazelcastCachingProvider() {
    }

    /**
     * Creates a {@link CacheManager} on an existing HazelcastInstance.
     *
     * @param uriString Scope of {@link CacheManager}
     * @param instance  Hazelcast instance that created {@link CacheManager} is connected.
     * @param props     Extra properties to be passed to cache manager. If {@code props} contain hazelcast.instance.name
     *                  it overrides {@code instance} parameter
     * @return the created {@link CacheManager}
     */
    public static CacheManager getCacheManager(HazelcastInstance instance, String uriString, Properties props) {
        URI uri = null;
        if (uriString != null) {
            try {
                uri = new URI(uriString);
            } catch (URISyntaxException e) {
                throw rethrow(e);
            }
        }
        if (instance instanceof HazelcastClientProxy) {
            return new HazelcastClientCachingProvider(instance).getCacheManager(uri, null, props);
        } else {
            return new HazelcastServerCachingProvider(instance).getCacheManager(uri, null, props);
        }
    }

    public static CacheManager getCacheManager(String uriString, Properties properties) {
        String instanceName = properties.getProperty(HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME);
        if (instanceName == null) {
            throw new IllegalStateException("Either 'instance-ref' attribute or "
                    + HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME + " property is required for creating cache manager");
        }
        return getCacheManager(Hazelcast.getHazelcastInstanceByName(instanceName), uriString, properties);
    }
}
