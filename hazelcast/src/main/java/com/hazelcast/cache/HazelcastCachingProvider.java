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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.Properties;

/**
 * Hazelcast implementation of JCache {@link javax.cache.spi.CachingProvider}.
 * <p>
 * This provider class is registered as a {@link CachingProvider} implementation.
 * When Hazelcast is the only {@link CachingProvider} on the classpath,
 * using {@link Caching#getCachingProvider()} will instantiate and return
 * an instance of this class.
 * <p>
 * This provider implementation delegates to a {@code CachingProvider} backed
 * by either a member- or a client-side {@link HazelcastInstance}:
 * <ul>
 * <li>{@link com.hazelcast.cache.HazelcastMemberCachingProvider} is the
 * member-side {@link CachingProvider} implementation</li>
 * <li>{@link com.hazelcast.client.cache.HazelcastClientCachingProvider} is the
 * client-side {@link CachingProvider} implementation</li>
 * </ul>
 * <h3>Provider Type Selection</h3>
 * <p>When using {@link Caching#getCachingProvider()} without a class name argument,
 * this provider is instantiated. The choice between member- or client-side provider
 * is made by inspecting the value of system property
 * {@code hazelcast.jcache.provider.type}:
 * <ul>
 *     <li>If no value was set, then the client-side caching provider is selected</li>
 *     <li>If a value was set, then value {@code member} selects the member-side caching
 *     provider, while value {@code client} selects the client-side provider. Legacy value
 *     {@code server} is also accepted as an alias for {@code member} for backwards compatibility,
 *     however its usage is discouraged and will be removed in a future version.
 *     Other values result in a {@link CacheException} being thrown.</li>
 * </ul>
 * <p>When using one of {@code Caching#getCachingProvider} variants with an explicit
 * class name argument, then:
 * <ul>
 *     <li>using {@code com.hazelcast.cache.HazelcastCachingProvider} as class name
 *     is identical to using {@link Caching#getCachingProvider()}; choice between
 *     member- or client-side caching provider is performed via system property
 *     {@code hazelcast.jcache.provider.type} as described above.</li>
 *     <li>using {@link #MEMBER_CACHING_PROVIDER} as
 *     class name will return a member-side caching provider</li>
 *     <li>using {@link #CLIENT_CACHING_PROVIDER} as
 *     class name will return a client-side caching provider</li>
 * </ul>
 * <h3>Creating or reusing HazelcastInstances with CacheManagers</h3>
 * Arguments used with {@link CachingProvider#getCacheManager(URI, ClassLoader, Properties)}
 * and its variants control whether a {@code HazelcastInstance} will be created or reused
 * to back the {@link CacheManager} being created:
 * <ul>
 *     <li>Property {@code hazelcast.config.location} specifies a URI to locate a Hazelcast
 *     member or client configuration file. Supports {@code classpath:}, {@code file:},
 *     {@code http:} and {@code https:} URI schemes.
 *     <b>Examples</b>: {@code classpath:com/acme/hazelcast.xml}
 *     will locate {@code hazelcast.xml} in package {@code com.acme},
 *     {@code http://internal.acme.com/hazelcast.xml} will locate the configuration from
 *     the given HTTP URL.</li>
 *     <li>Property {@code hazelcast.instance.name} specifies the instance name of a running
 *     {@code HazelcastInstance}. If no instance is found running by that name, then a new
 *     {@code HazelcastInstance} is started with a default configuration and the given
 *     instance name.</li>
 *     <li>In any {@code CachingProvider#getCacheManager} variant that accepts a
 *     {@link URI} as argument, and if no properties were provided or properties did not result
 *     in resolving a specific {@code HazelcastInstance}, then the {@code URI} argument is
 *     interpreted as a Hazelcast config location as follows:
 *      <ol>
 *          <li>if {@code URI} starts with one of supported schemes ({@code classpath:}, {@code http:},
 *          {@code https:}, {@code file:}), then a Hazelcast XML configuration is loaded from that location.</li>
 *          <li>otherwise, {@code URI} is interpreted as a system property. If {@link System#getProperty(String)
 *          System.getProperty(URI)} returns a value that starts with one of supported schemes above, then
 *          a Hazelcast XML configuration is loaded from that location.</li>
 *          <li>if {@code URI} or its resolved value as a system property does not start with a supported
 *          URI scheme, a default {@code HazelcastInstance} named {@value #SHARED_JCACHE_INSTANCE_NAME} is
 *          created or used, if it already exists.</li>
 *      </ol>
 *     </li>
 * </ul>
 * Convenience methods {@link #propertiesByLocation(String)} and {@link #propertiesByInstanceName(String)}
 * will create an appropriate {@link Properties} instance for use with
 * {@link #getCacheManager(URI, ClassLoader, Properties)}.
 * <h3>Examples</h3>
 * <p><b>Obtain a member-side caching provider backed by an existing HazelcastInstance.</b>
 * In this example the member-side caching provider is selected by setting the value of
 * system property {@code hazelcast.jcache.provider.type} to value "{@code member}". An existing
 * {@code HazelcastInstance} is referenced by instance name in the {@code Properties} provided as
 * argument to {@link CachingProvider#getCacheManager(URI, ClassLoader, Properties)}.
 * <blockquote><pre>
 * Config config = new Config();
 * config.setInstanceName("hz-jcache");
 * HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
 *
 * System.setProperty("hazelcast.jcache.provider.type", "member");
 * CachingProvider provider = Caching.getCachingProvider();
 * CacheManager manager = provider.getCacheManager(null, null, HazelcastCachingProvider.propertiesByInstanceName("hz-jcache"));
 * Cache cache = manager.createCache("sessions", new MutableConfiguration());
 * cache.put("a", "b");
 * </pre>
 * </blockquote>
 * <p><b>Obtain a client-side caching provider, starting a default client {@code HazelcastInstance}</b>
 * In this example the client-side caching provider is selected as default option. A new
 * client-side {@code HazelcastInstance} is created with default configuration once
 * {@link CachingProvider#getCacheManager()} is called.
 * <blockquote><pre>
 * // start a Hazelcast member for the client to connect to
 * HazelcastInstance member = Hazelcast.newHazelcastInstance();
 *
 * // obtain a client-side (default) CachingProvider
 * CachingProvider provider = Caching.getCachingProvider();
 * // obtain the default CacheManager; since there is no JCache-backing client-side
 * // HazelcastInstance started, this will start a new instance
 * CacheManager manager = provider.getCacheManager();
 * Cache cache = manager.createCache("sessions", new MutableConfiguration());
 * cache.put("a", "b");
 * </pre>
 * </blockquote>
 *
 * @since 3.4
 */
public final class HazelcastCachingProvider implements CachingProvider {

    /**
     * Hazelcast config location property
     */
    public static final String HAZELCAST_CONFIG_LOCATION = "hazelcast.config.location";

    /**
     * Hazelcast instance name property
     */
    public static final String HAZELCAST_INSTANCE_NAME = "hazelcast.instance.name";

    /**
     * Hazelcast instance itself property
     */
    public static final String HAZELCAST_INSTANCE_ITSELF = "hazelcast.instance.itself";

    /**
     * Class name of the member-side Caching Provider
     */
    public static final String MEMBER_CACHING_PROVIDER = HazelcastMemberCachingProvider.class.getName();

    /**
     * Same value as {@link #MEMBER_CACHING_PROVIDER}. This field is maintained for backwards compatibility.
     * Its use is discouraged and will be removed in a future version.
     */
    @Deprecated
    public static final String SERVER_CACHING_PROVIDER = HazelcastMemberCachingProvider.class.getName();

    /**
     * Class name of the client-side Caching Provider
     */
    public static final String CLIENT_CACHING_PROVIDER =
            com.hazelcast.client.cache.HazelcastClientCachingProvider.class.getName();

    /**
     * Name of default {@link HazelcastInstance} which may be started when
     * obtaining the default {@link CachingProvider}.
     */
    public static final String SHARED_JCACHE_INSTANCE_NAME = "_hzinstance_jcache_shared";

    private static final String PROVIDER_TYPE_CLIENT = "client";
    private static final String PROVIDER_TYPE_MEMBER = "member";
    private static final String LEGACY_PROVIDER_TYPE_MEMBER = "server";

    private final CachingProvider delegate;

    public HazelcastCachingProvider() {
        CachingProvider cp = null;
        String providerType = ClusterProperty.JCACHE_PROVIDER_TYPE.getSystemProperty();
        if (providerType != null) {
            if (PROVIDER_TYPE_CLIENT.equals(providerType)) {
                cp = new HazelcastClientCachingProvider();
            } else if (PROVIDER_TYPE_MEMBER.equals(providerType)
                    || LEGACY_PROVIDER_TYPE_MEMBER.equals(providerType)) {
                cp = new HazelcastServerCachingProvider();
            } else {
                throw new CacheException("Unknown CachingProvider type \"" + providerType + "\". Use "
                        + "\"client\" or \"member\" as provider type.");
            }
        } else {
            cp = new HazelcastClientCachingProvider();
        }
        delegate = cp;
    }

    /**
     * Create the {@link java.util.Properties} with the provided config file location.
     *
     * @param configFileLocation the location of the config file to configure
     * @return properties instance pre-configured with the configuration location
     */
    public static Properties propertiesByLocation(String configFileLocation) {
        Properties properties = new Properties();
        properties.setProperty(HAZELCAST_CONFIG_LOCATION, configFileLocation);
        return properties;
    }

    /**
     * Create the {@link java.util.Properties} with the provided instance name.
     *
     * @param instanceName the instance name to configure
     * @return the properties instance pre-configured with the instance name
     */
    public static Properties propertiesByInstanceName(String instanceName) {
        Properties properties = new Properties();
        properties.setProperty(HAZELCAST_INSTANCE_NAME, instanceName);
        return properties;
    }

    /**
     * Create the {@link java.util.Properties} with the provided instance itself.
     *
     * @param instance the instance itself to be used
     * @return the properties instance pre-configured with the instance itself
     */
    public static Properties propertiesByInstanceItself(HazelcastInstance instance) {
        Properties properties = new Properties();
        properties.put(HAZELCAST_INSTANCE_ITSELF, instance);
        return properties;
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
        return "HazelcastCachingProvider{delegate=" + delegate + '}';
    }
}
