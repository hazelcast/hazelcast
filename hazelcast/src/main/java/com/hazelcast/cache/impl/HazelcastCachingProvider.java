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

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ClassLoaderUtil;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.Properties;

/**
 * Main {@link CachingProvider} implementation to provide Hazelcast JCache Implementation.
 * <p>Main purpose of this provider implementation is delegating to selected internal actual provider
 * implementation.</p>
 * <p>There are two internal {@link CachingProvider}s:
 * <ol>
 *     <li>{@link HazelcastServerCachingProvider}</li>
 *     <li>HazelcastClientCachingProvider</li>
 * </ol>
 * </p>
 * <p>
 * <h3>Provider Type Selection:</h3>
 * First step is to check whether a selection exists using the system property
 * <pre>hazelcast.jcache.provider.type</pre> with values "client" or "server".
 * If no selection exists, then the default behavior for selecting the internal provider type is based on
 * which dependency found on classpath. Client and server provider classes are searched on classpath respectively.
 * </p>
 */
public final class HazelcastCachingProvider
        implements CachingProvider {

    private static final String CLIENT_CACHING_PROVIDER_CLASS = "com.hazelcast.client.cache.impl.HazelcastClientCachingProvider";
    private static final ILogger LOGGER = Logger.getLogger(HazelcastCachingProvider.class);

    private final CachingProvider delegate;

    public HazelcastCachingProvider() {
        CachingProvider cp = null;
        String providerType = System.getProperty(GroupProperties.PROP_JCACHE_PROVIDER_TYPE);
        if (providerType != null) {
            if ("client".equals(providerType)) {
                cp = createClientProvider();
            }
            if ("server".equals(providerType)) {
                cp = new HazelcastServerCachingProvider();
            }
            if (cp == null) {
                throw new CacheException("CacheProvider cannot created with the provided type:" + providerType);
            }
        } else {
            cp = createClientProvider();
            if (cp == null) {
                cp = new HazelcastServerCachingProvider();
            }
        }
        delegate = cp;
    }

    private CachingProvider createClientProvider() {
        try {
            return ClassLoaderUtil.newInstance(getClass().getClassLoader(), CLIENT_CACHING_PROVIDER_CLASS);
        } catch (Exception e) {
            LOGGER.warning("Could not load client CachingProvider! Fallback to server one... " + e.toString());
        }
        return null;
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HazelcastCachingProvider{");
        sb.append("delegate=").append(delegate);
        sb.append('}');
        return sb.toString();
    }
}
