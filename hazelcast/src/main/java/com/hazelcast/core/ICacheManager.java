/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.cache.ICache;

/**
 * {@code ICacheManager} is the entry point to access JSR-107 (JCache) caches via {@link HazelcastInstance} interface.
 * Hazelcast's {@code ICacheManager} provides access to JCache caches configured cluster-wide, even when created by different
 * JCache {@link javax.cache.CacheManager}s.
 *
 * <p>Note that this interface is not related to JCache {@link javax.cache.CacheManager}. Its purpose is to host
 * {@code ICache} related methods, separately from {@link HazelcastInstance}, in order to allow frameworks that make
 * use of reflection and/or dynamic proxies (e.g. Mockito, Spring etc) to operate on {@link HazelcastInstance} when JCache
 * is not on the classpath.
 * See also related issue https://github.com/hazelcast/hazelcast/issues/8352.
 * </p>
 *
 * @since 3.7
 */
public interface ICacheManager {

    /**
     * <p>
     * Returns the cache instance with the specified prefixed cache name.
     * </p>
     *
     * <p>
     * Prefixed cache name is the name with URI and classloader prefixes if available.
     * There is no Hazelcast prefix ({@code /hz/}). For example, {@code myURI/foo}.
     *
     * <pre>
     * <code>
     *     &lt;prefixed_cache_name&gt; = [&lt;uri_prefix&gt;/] + [&lt;cl_prefix&gt;/] + &lt;pure_cache_name&gt;
     * </code>
     * </pre>
     * where {@code <pure_cache_name>} is the cache name without any prefix. For example {@code foo}.
     *
     * As seen from the definition, URI and classloader prefixes are optional.
     *
     * URI prefix is generated as content of this URI as a US-ASCII string. ({@code uri.toASCIIString()})
     * Classloader prefix is generated as string representation of the specified classloader. ({@code cl.toString()})
     *
     * @see com.hazelcast.cache.CacheUtil#getPrefixedCacheName(String, java.net.URI, ClassLoader)
     * </p>
     *
     * @param name the prefixed name of the cache
     * @return the cache instance with the specified prefixed name
     *
     * @throws com.hazelcast.cache.CacheNotExistsException  if there is no configured or created cache
     *                                                      with the specified prefixed name
     * @throws java.lang.IllegalStateException              if a valid JCache library does not exist in the classpath
     *                                                      ({@code 1.0.0-PFD} or {@code 0.x} versions are not valid)
     */
    <K, V> ICache<K, V> getCache(String name);
}
