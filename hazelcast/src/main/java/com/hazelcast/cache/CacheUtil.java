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

package com.hazelcast.cache;

import java.net.URI;

/**
 * Utility class for various cache related operations to be used by our internal structure and end user.
 */
public final class CacheUtil {

    private CacheUtil() {
    }

    /**
     * Gets the prefix of cache name without Hazelcast's {@link javax.cache.CacheManager}
     * specific prefix {@link com.hazelcast.cache.HazelcastCacheManager#CACHE_MANAGER_PREFIX}.
     *
     * @param uri           an implementation specific URI for the
     *                      Hazelcast's {@link javax.cache.CacheManager} (null means use
     *                      Hazelcast's {@link javax.cache.spi.CachingProvider#getDefaultURI()})
     * @param classLoader   the {@link ClassLoader}  to use for the
     *                      Hazelcast's {@link javax.cache.CacheManager} (null means use
     *                      Hazelcast's {@link javax.cache.spi.CachingProvider#getDefaultClassLoader()})
     * @return  the prefix of cache name without Hazelcast's {@link javax.cache.CacheManager}
     *          specific prefix {@link com.hazelcast.cache.HazelcastCacheManager#CACHE_MANAGER_PREFIX}
     */
    public static String getPrefix(URI uri, ClassLoader classLoader) {
        if (uri == null && classLoader == null) {
            return null;
        } else {
            StringBuilder sb = new StringBuilder();
            if (uri != null) {
                sb.append(uri.toASCIIString()).append('/');
            }
            if (classLoader != null) {
                sb.append(classLoader.toString()).append('/');
            }
            return sb.toString();
        }
    }

    /**
     * Gets the cache name with prefix but without Hazelcast's {@link javax.cache.CacheManager}
     * specific prefix {@link com.hazelcast.cache.HazelcastCacheManager#CACHE_MANAGER_PREFIX}.
     *
     * @param name          the simple name of the cache without any prefix
     * @param uri           an implementation specific URI for the
     *                      Hazelcast's {@link javax.cache.CacheManager} (null means use
     *                      Hazelcast's {@link javax.cache.spi.CachingProvider#getDefaultURI()})
     * @param classLoader   the {@link ClassLoader}  to use for the
     *                      Hazelcast's {@link javax.cache.CacheManager} (null means use
     *                      Hazelcast's {@link javax.cache.spi.CachingProvider#getDefaultClassLoader()})
     * @return  the cache name with prefix but without Hazelcast's {@link javax.cache.CacheManager}
     *          specific prefix {@link com.hazelcast.cache.HazelcastCacheManager#CACHE_MANAGER_PREFIX}.
     */
    public static String getPrefixedCacheName(String name, URI uri, ClassLoader classLoader) {
        String cacheNamePrefix = getPrefix(uri, classLoader);
        if (cacheNamePrefix != null) {
            return cacheNamePrefix + name;
        } else {
            return name;
        }
    }

}
