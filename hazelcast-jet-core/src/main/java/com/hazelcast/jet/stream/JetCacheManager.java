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

package com.hazelcast.jet.stream;


import com.hazelcast.jet.JetInstance;

/**
 * {@code JetCacheManager} is the entry point to access JSR-107 (JCache) caches
 * via {@link JetInstance} interface. Hazelcast Jet's {@code JetCacheManager}
 * provides access to JCache caches configured cluster-wide,
 * even when created by different JCache {@link javax.cache.CacheManager}s.
 * <p>
 * Note that this interface is not related to JCache {@link javax.cache.CacheManager}.
 * Its purpose is to host {@code IStreamCache} related methods, separately from
 * {@link JetInstance}, in order to allow frameworks that make
 * use of reflection and/or dynamic proxies (e.g. Mockito, Spring etc)
 * to operate on {@link JetInstance} when JCache is not on the classpath.
 * </p>
 *
 * @since 0.5
 */
public interface JetCacheManager {

    /**
     * Returns the cache instance with the specified, optionally prefixed, cache
     * name:
     * <pre>
     * &lt;prefixed_cache_name&gt; = [&lt;uri_prefix&gt;/][&lt;cl_prefix&gt;/]&lt;simple_cache_name&gt;
     * </pre>
     * where {@code <simple_cache_name>} is the cache name without any prefix.
     * <p>
     * @see com.hazelcast.cache.CacheUtil#getPrefixedCacheName(String, java.net.URI, ClassLoader)
     *
     * @param name the prefixed name of the cache
     * @return the cache instance with the specified prefixed name
     *
     * @throws com.hazelcast.cache.CacheNotExistsException  if there is no configured or created cache
     *                                                      with the specified prefixed name
     * @throws java.lang.IllegalStateException              if a valid JCache library does not exist in the classpath
     *                                                      ({@code 1.0.0-PFD} or {@code 0.x} versions are not valid)
     */
    <K, V> IStreamCache<K, V> getCache(String name);
}
