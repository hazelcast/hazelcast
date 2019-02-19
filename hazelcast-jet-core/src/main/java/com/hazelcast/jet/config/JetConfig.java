/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.config;

import com.hazelcast.config.Config;
import com.hazelcast.jet.impl.config.XmlJetConfigBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;
import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration object for a Jet instance.
 */
public class JetConfig {

    /**
     * The default port number for the cluster auto-discovery mechanism's
     * multicast communication.
     */
    public static final int DEFAULT_JET_MULTICAST_PORT = 54326;

    /**
     * The default group name for a Jet cluster
     *
     * See {@link com.hazelcast.config.GroupConfig}
     */
    public static final String DEFAULT_GROUP_NAME = "jet";

    /**
     * The default group password for a Jet cluster.
     *
     * See {@link com.hazelcast.config.GroupConfig}
     */
    public static final String DEFAULT_GROUP_PASSWORD = "jet-pass";

    private static final ILogger LOGGER = Logger.getLogger(JetConfig.class);

    private Config hazelcastConfig = defaultHazelcastConfig();
    private InstanceConfig instanceConfig = new InstanceConfig();
    private EdgeConfig defaultEdgeConfig = new EdgeConfig();
    private MetricsConfig metricsConfig = new MetricsConfig();
    private Properties properties = new Properties();

    /**
     * Creates a new, empty {@code JetConfig} with the default configuration.
     * Doesn't consider any configuration XML files.
     */
    public JetConfig() {
    }

    /**
     * Loads JetConfig using the default lookup mechanism to locate the
     * configuration file. Loads the nested {@linkplain #getHazelcastConfig()
     * Hazelcast config} also by using its default lookup mechanism. Uses
     * {@code System.getProperties()} to resolve the variables in the XML.
     * <p>
     * This is the lookup mechanism for the Jet configuration:
     * <ol><li>
     *     Read the system property {@code hazelcast.jet.config}. If it starts with
     *     {@code classpath:}, treat it as a classpath resource, otherwise it's a
     *     file pathname. If it's defined but Jet can't find the file it specifies,
     *     startup fails.
     * </li><li>
     *     Look for {@code hazelcast-jet.xml} in the working directory.
     * </li><li>
     *     Look for {@code hazelcast-jet.xml} in the classpath.
     * </li><li>
     *     Load the default XML configuration packaged in Jet's JAR.
     * </li></ol>
     * The mechanism is the same for the nested Hazelcast config, just with the
     * {@code hazelcast.config} system property.
     */
    @Nonnull
    public static JetConfig loadDefault() {
        return XmlJetConfigBuilder.loadConfig(null, null);
    }

    /**
     * Loads JetConfig using the built-in {@link #loadDefault lookup mechanism}
     * to locate the configuration file. Loads the nested {@linkplain
     * #getHazelcastConfig() Hazelcast config} also by using the lookup
     * mechanism. Uses the given {@code properties} to resolve the variables in
     * the XML.
     */
    @Nonnull
    public static JetConfig loadDefault(@Nonnull Properties properties) {
        return XmlJetConfigBuilder.loadConfig(null, properties);
    }

    /**
     * Uses the thread's context class loader to load JetConfig from the
     * classpath resource named by the argument. Uses {@code
     * System.getProperties()} to resolve the variables in the XML.
     * <p>
     * This method loads the nested {@linkplain #getHazelcastConfig() Hazelcast
     * config} using the built-in {@link #loadDefault lookup mechanism}, but
     * you can replace it afterwards by calling {@link #setHazelcastConfig
     * setHazelcastConfig()} with, for example, {@link
     * com.hazelcast.config.ClasspathXmlConfig ClasspathXmlConfig} or {@link
     * com.hazelcast.config.FileSystemXmlConfig FileSystemXmlConfig}.
     *
     * @param resource names the classpath resource containing the XML configuration file
     *
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     * @throws IllegalArgumentException if classpath resource is not found
     */
    @Nonnull
    public static JetConfig loadFromClasspath(@Nonnull String resource) {
        return loadFromClasspath(resource, System.getProperties());
    }

    /**
     * Uses the thread's context class loader to load JetConfig from the
     * classpath resource named by the argument. Uses the given {@code
     * properties} to resolve the variables in the XML.
     * <p>
     * This method loads the nested {@linkplain #getHazelcastConfig() Hazelcast
     * config} using the built-in {@link #loadDefault lookup mechanism}, but
     * you can replace it afterwards by calling {@link #setHazelcastConfig
     * setHazelcastConfig()} with, for example, {@link
     * com.hazelcast.config.ClasspathXmlConfig ClasspathXmlConfig} or {@link
     * com.hazelcast.config.FileSystemXmlConfig FileSystemXmlConfig}.
     *
     * @param resource the classpath resource, an XML configuration file on the
     *      classpath
     *
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     * @throws IllegalArgumentException if classpath resource is not found
     */
    @Nonnull
    public static JetConfig loadFromClasspath(@Nonnull String resource, @Nonnull Properties properties) {
        LOGGER.info("Configuring Hazelcast Jet from '" + resource + "' on classpath");
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
        if (stream == null) {
            throw new IllegalArgumentException("Specified resource '" + resource + "' cannot be found on classpath");
        }
        return loadFromStream(stream, properties);
    }

    /**
     * Loads JetConfig from the supplied input stream. Uses {@code
     * System.getProperties()} to resolve the variables in the XML.
     * <p>
     * This method loads the nested {@linkplain #getHazelcastConfig() Hazelcast
     * config} using the built-in {@link #loadDefault lookup mechanism}, but
     * you can replace it afterwards by calling {@link #setHazelcastConfig
     * setHazelcastConfig()} with, for example, {@link
     * com.hazelcast.config.ClasspathXmlConfig ClasspathXmlConfig} or {@link
     * com.hazelcast.config.FileSystemXmlConfig FileSystemXmlConfig}.
     *
     * @param configStream the InputStream to load the config from
     *
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    @Nonnull
    public static JetConfig loadFromStream(@Nonnull InputStream configStream) {
        return loadFromStream(configStream, System.getProperties());
    }

    /**
     * Loads JetConfig from the supplied input stream. Uses the given {@code
     * properties} to resolve the variables in the XML.
     * <p>
     * This method loads the nested {@linkplain #getHazelcastConfig() Hazelcast
     * config} using the built-in {@link #loadDefault lookup mechanism}, but
     * you can replace it afterwards by calling {@link #setHazelcastConfig
     * setHazelcastConfig()} with, for example, {@link
     * com.hazelcast.config.ClasspathXmlConfig ClasspathXmlConfig} or {@link
     * com.hazelcast.config.FileSystemXmlConfig FileSystemXmlConfig}.
     *
     * @param configStream the InputStream to load the config from
     * @param properties the properties to resolve variables in the XML
     *
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    @Nonnull
    public static JetConfig loadFromStream(@Nonnull InputStream configStream, @Nonnull Properties properties) {
        return XmlJetConfigBuilder.loadConfig(configStream, properties);
    }

    /**
     * Returns the configuration object for the underlying Hazelcast IMDG
     * instance.
     */
    @Nonnull
    public Config getHazelcastConfig() {
        return hazelcastConfig;
    }

    /**
     * Sets the underlying Hazelcast IMDG instance's configuration object.
     */
    @Nonnull
    public JetConfig setHazelcastConfig(@Nonnull Config config) {
        Preconditions.checkNotNull(config, "config");
        hazelcastConfig = config;
        return this;
    }

    /**
     * Returns the Jet instance config.
     */
    @Nonnull
    public InstanceConfig getInstanceConfig() {
        return instanceConfig;
    }

    /**
     * Sets the Jet instance config.
     */
    @Nonnull
    public JetConfig setInstanceConfig(@Nonnull InstanceConfig instanceConfig) {
        Preconditions.checkNotNull(instanceConfig, "instanceConfig");
        this.instanceConfig = instanceConfig;
        return this;
    }

    /**
     * Returns the metrics collection config.
     */
    @Nonnull
    public MetricsConfig getMetricsConfig() {
        return metricsConfig;
    }

    /**
     * Sets the metrics collection config.
     */
    @Nonnull
    public JetConfig setMetricsConfig(@Nonnull MetricsConfig metricsConfig) {
        Preconditions.checkNotNull(metricsConfig, "metricsConfig");
        this.metricsConfig = metricsConfig;
        return this;
    }

    /**
     * Returns the Jet-specific configuration properties.
     */
    @Nonnull
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the Jet-specific configuration properties.
     */
    @Nonnull
    public JetConfig setProperties(@Nonnull Properties properties) {
        Preconditions.checkNotNull(properties, "properties");
        this.properties = properties;
        return this;
    }

    /**
     * Sets the value of the specified property.
     */
    @Nonnull
    public JetConfig setProperty(@Nonnull String name, @Nonnull String value) {
        Preconditions.checkNotNull(name, "name");
        Preconditions.checkNotNull(value, "value");
        properties.put(name, value);
        return this;
    }

    /**
     * Returns the default DAG edge configuration.
     */
    @Nonnull
    public EdgeConfig getDefaultEdgeConfig() {
        return defaultEdgeConfig;
    }

    /**
     * Sets the configuration object that specifies the defaults to use
     * for a DAG edge configuration.
     */
    @Nonnull
    public JetConfig setDefaultEdgeConfig(@Nonnull EdgeConfig defaultEdgeConfig) {
        Preconditions.checkNotNull(defaultEdgeConfig, "defaultEdgeConfig");
        this.defaultEdgeConfig = defaultEdgeConfig;
        return this;
    }

    private static Config defaultHazelcastConfig() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setMulticastPort(DEFAULT_JET_MULTICAST_PORT);
        config.getGroupConfig().setName(DEFAULT_GROUP_NAME);
        config.getGroupConfig().setPassword(DEFAULT_GROUP_PASSWORD);
        return config;
    }
}
