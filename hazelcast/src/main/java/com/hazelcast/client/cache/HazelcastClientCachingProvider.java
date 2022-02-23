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

package com.hazelcast.client.cache;

import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.Properties;

/**
 * Client-side {@link CachingProvider} implementation. Its fully-qualified
 * class name can be used to request the client-side {@code CachingProvider}
 * implementation. Examples:
 * <ul>
 *     <li>
 *         Programmatically, using {@link javax.cache.Caching#getCachingProvider(String)}:
 * <pre>
 * {@code
 * CachingProvider clientSideCachingProvider =
 *     Caching.getCachingProvider("com.hazelcast.client.cache.HazelcastClientCachingProvider");
 * }
 * </pre>
 *     </li>
 *     <li>
 *         Declaratively, using the {@code javax.cache.spi.CachingProvider} system property
 * as specified in {@link javax.cache.Caching}.
 * <pre>
 * {@code
 * // alternatively, set the system property on the java command line:
 * // java -Djavax.cache.spi.CachingProvider=com.hazelcast.client.cache.HazelcastClientCachingProvider
 * System.setProperty("javax.cache.spi.CachingProvider", "com.hazelcast.client.cache.HazelcastClientCachingProvider");
 * CachingProvider clientSideCachingProvider = Caching.getCachingProvider();
 * }
 * </pre>
 *     </li>
 * </ul>
 */
public class HazelcastClientCachingProvider implements CachingProvider {

    private final CachingProvider delegate;

    public HazelcastClientCachingProvider() {
        delegate = new com.hazelcast.client.cache.impl.HazelcastClientCachingProvider();
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        return delegate.getCacheManager(uri, classLoader, properties);
    }

    @Override
    public ClassLoader getDefaultClassLoader() {
        return delegate.getDefaultClassLoader();
    }

    @Override
    public URI getDefaultURI() {
        return delegate.getDefaultURI();
    }

    @Override
    public Properties getDefaultProperties() {
        return delegate.getDefaultProperties();
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
        return delegate.getCacheManager(uri, classLoader);
    }

    @Override
    public CacheManager getCacheManager() {
        return delegate.getCacheManager();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void close(ClassLoader classLoader) {
        delegate.close(classLoader);
    }

    @Override
    public void close(URI uri, ClassLoader classLoader) {
        delegate.close(uri, classLoader);
    }

    @Override
    public boolean isSupported(OptionalFeature optionalFeature) {
        return delegate.isSupported(optionalFeature);
    }
}
