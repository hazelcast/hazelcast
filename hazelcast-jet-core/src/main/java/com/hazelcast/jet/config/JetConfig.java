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
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.jet.impl.config.ConfigProvider;
import com.hazelcast.jet.impl.config.XmlJetConfigBuilder;
import com.hazelcast.jet.impl.config.YamlJetConfigBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.jet.impl.util.JetProperties.JET_HOME;
import static com.hazelcast.util.Preconditions.checkTrue;
import static com.hazelcast.util.StringUtil.isNullOrEmptyAfterTrim;
import static com.hazelcast.util.StringUtil.stringToBytes;

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

    private static final ILogger LOGGER = Logger.getLogger(JetConfig.class);

    static {
        String value = jetHome();
        LOGGER.info("jet.home is " + value);
        System.setProperty(JET_HOME.getName(), value);
    }

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
     * {@code System.getProperties()} to resolve the variables in the XML or YAML.
     * <p>
     * This is the lookup mechanism for the Jet configuration:
     * <ol><li>
     * Read the system property {@code hazelcast.jet.config}. If it starts with
     * {@code classpath:}, treat it as a classpath resource, otherwise it's a
     * file pathname. If it's defined but Jet can't find the file it specifies,
     * startup fails.
     * </li><li>
     * Look for {@code hazelcast-jet.xml} in the working directory.
     * </li><li>
     * Look for {@code hazelcast-jet.xml} in the classpath.
     * </li><li>
     * Look for {@code hazelcast-jet.yaml} in the working directory.
     * </li><li>
     * Look for {@code hazelcast-jet.yaml} in the classpath.
     * </li><li>
     * Load the default XML configuration packaged in Jet's JAR.
     * </li></ol>
     * The mechanism is the same for the nested Hazelcast config, just with the
     * {@code hazelcast.config} system property.
     */
    @Nonnull
    public static JetConfig loadDefault() {
        return ConfigProvider.locateAndGetJetConfig();
    }

    /**
     * Loads JetConfig using the built-in {@link #loadDefault lookup mechanism}
     * to locate the configuration file. Loads the nested {@linkplain
     * #getHazelcastConfig() Hazelcast config} also by using the lookup
     * mechanism. Uses the given {@code properties} to resolve the variables in
     * the XML or YAML.
     */
    @Nonnull
    public static JetConfig loadDefault(Properties properties) {
        checkTrue(properties != null, "properties can't be null");
        return ConfigProvider.locateAndGetJetConfig(properties);
    }

    /**
     * Creates a JetConfig which is loaded from a classpath resource. The
     * System.properties are used to resolve variables in the configuration file.
     *
     * @param classLoader the ClassLoader used to load the resource
     * @param resource    the resource, an XML or YAML configuration file from
     *                    the classpath, without the "classpath:" prefix
     * @throws IllegalArgumentException      if classLoader or resource is {@code null},
     *                                       or if the resource is not found
     * @throws InvalidConfigurationException if the resource content is invalid
     */
    @Nonnull
    public static JetConfig loadFromClasspath(ClassLoader classLoader, String resource) {
        return loadFromClasspath(classLoader, resource, System.getProperties());
    }

    /**
     * Creates a JetConfig which is loaded from a classpath resource. Uses the
     * given {@code properties} to resolve the variables in the resource.
     *
     * @param classLoader the ClassLoader used to load the resource
     * @param resource    the resource, an XML or YAML configuration file from
     *                    the classpath, without the "classpath:" prefix
     * @param properties  the properties used to resolve variables in the resource
     * @throws IllegalArgumentException      if classLoader or resource is {@code null},
     *                                       or if the resource is not found
     * @throws InvalidConfigurationException if the resource content is invalid
     */
    @Nonnull
    public static JetConfig loadFromClasspath(ClassLoader classLoader, String resource, Properties properties) {
        checkTrue(classLoader != null, "classLoader can't be null");
        checkTrue(resource != null, "resource can't be null");
        checkTrue(properties != null, "properties can't be null");

        LOGGER.info("Configuring Hazelcast Jet from '" + resource + "'.");
        InputStream in = classLoader.getResourceAsStream(resource);
        checkTrue(in != null, "Specified resource '" + resource + "' could not be found!");
        if (resource.endsWith(".xml")) {
            return loadXmlFromStream(in, properties);
        } else if (resource.endsWith(".yaml") || resource.endsWith(".yml")) {
            return loadYamlFromStream(in, properties);
        } else {
            throw new IllegalArgumentException("Unknown configuration file extension");
        }
    }

    /**
     * Creates a JetConfig based on a Hazelcast Jet configuration file (XML or YAML)
     * and uses the System.properties to resolve variables in the file.
     *
     * @param configFile the path of the Hazelcast Jet configuration file
     * @throws FileNotFoundException         if the file doesn't exist
     * @throws InvalidConfigurationException if the file content is invalid
     */
    @Nonnull
    public static JetConfig loadFromFile(File configFile) throws FileNotFoundException {
        return loadFromFile(configFile, System.getProperties());
    }

    /**
     * Creates a JetConfig based on a Hazelcast Jet configuration file (XML or YAML).
     * Uses the given {@code properties} to resolve the variables in the file.
     *
     * @param configFile the path of the Hazelcast Jet configuration file
     * @param properties the Properties to resolve variables in the file
     * @throws IllegalArgumentException      if configFile or properties is {@code null}
     * @throws FileNotFoundException         if the file doesn't exist
     * @throws InvalidConfigurationException if the file content is invalid
     */
    @Nonnull
    public static JetConfig loadFromFile(File configFile, Properties properties) throws FileNotFoundException {
        checkTrue(configFile != null, "configFile can't be null");
        checkTrue(properties != null, "properties can't be null");

        LOGGER.info("Configuring Hazelcast Jet from '" + configFile.getAbsolutePath() + "'.");
        String path = configFile.getPath();
        InputStream in = new FileInputStream(configFile);
        if (path.endsWith(".xml")) {
            return loadXmlFromStream(in, properties);
        } else if (path.endsWith(".yaml") || path.endsWith(".yml")) {
            return loadYamlFromStream(in, properties);
        } else {
           throw new IllegalArgumentException("Unknown configuration file extension");
        }
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
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    @Nonnull
    public static JetConfig loadXmlFromStream(@Nonnull InputStream configStream) {
        return loadXmlFromStream(configStream, System.getProperties());
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
     * @param properties   the properties to resolve variables in the XML
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     */
    @Nonnull
    public static JetConfig loadXmlFromStream(@Nonnull InputStream configStream, @Nonnull Properties properties) {
        return new XmlJetConfigBuilder(configStream).setProperties(properties).build();
    }

    /**
     * Creates a JetConfig from the provided XML string and uses the
     * System.properties to resolve variables in the XML.
     *
     * @param xml the XML content as a Hazelcast Jet XML String
     * @throws IllegalArgumentException      if the XML is null or empty
     * @throws InvalidConfigurationException if the XML content is invalid
     */
    @Nonnull
    public static JetConfig loadXmlFromString(String xml) {
        return loadXmlFromString(xml, System.getProperties());
    }

    /**
     * Creates a JetConfig from the provided XML string and properties to resolve
     * the variables in the XML.
     *
     * @param xml the XML content as a Hazelcast Jet XML String
     * @throws IllegalArgumentException      if the XML is null or empty or if
     *                                       properties is null
     * @throws InvalidConfigurationException if the XML content is invalid
     */
    @Nonnull
    public static JetConfig loadXmlFromString(String xml, Properties properties) {
        LOGGER.info("Configuring Hazelcast Jet from 'in-memory xml'.");
        if (isNullOrEmptyAfterTrim(xml)) {
            throw new IllegalArgumentException("XML configuration is null or empty! Please use a well-structured xml.");
        }
        checkTrue(properties != null, "properties can't be null");
        InputStream in = new ByteArrayInputStream(stringToBytes(xml));
        return new XmlJetConfigBuilder(in).setProperties(properties).build();
    }

    /**
     * Loads JetConfig from the supplied input stream. Uses {@code
     * System.getProperties()} to resolve the variables in the YAML.
     * <p>
     * This method loads the nested {@linkplain #getHazelcastConfig() Hazelcast
     * config} using the built-in {@link #loadDefault lookup mechanism}, but
     * you can replace it afterwards by calling {@link #setHazelcastConfig
     * setHazelcastConfig()} with, for example, {@link
     * com.hazelcast.config.ClasspathYamlConfig ClasspathYamlConfig} or {@link
     * com.hazelcast.config.FileSystemYamlConfig FileSystemYamlConfig}.
     *
     * @param configStream the InputStream to load the config from
     * @throws com.hazelcast.core.HazelcastException if the YAML content is invalid
     */
    @Nonnull
    public static JetConfig loadYamlFromStream(@Nonnull InputStream configStream) {
        return loadYamlFromStream(configStream, System.getProperties());
    }

    /**
     * Loads JetConfig from the supplied input stream. Uses the given {@code
     * properties} to resolve the variables in the YAML.
     * <p>
     * This method loads the nested {@linkplain #getHazelcastConfig() Hazelcast
     * config} using the built-in {@link #loadDefault lookup mechanism}, but
     * you can replace it afterwards by calling {@link #setHazelcastConfig
     * setHazelcastConfig()} with, for example, {@link
     * com.hazelcast.config.ClasspathYamlConfig ClasspathYamlConfig} or {@link
     * com.hazelcast.config.FileSystemYamlConfig FileSystemYamlConfig}.
     *
     * @param configStream the InputStream to load the config from
     * @throws com.hazelcast.core.HazelcastException if the YAML content is invalid
     */
    @Nonnull
    public static JetConfig loadYamlFromStream(@Nonnull InputStream configStream, @Nonnull Properties properties) {
        return new YamlJetConfigBuilder(configStream).setProperties(properties).build();
    }

    /**
     * Creates a JetConfig from the provided YAML string and uses the
     * System.properties to resolve variables in the YAML.
     *
     * @param yaml the YAML content as a Hazelcast Jet YAML String
     * @throws IllegalArgumentException      if the YAML is null or empty
     * @throws InvalidConfigurationException if the YAML content is invalid
     */
    @Nonnull
    public static JetConfig loadYamlFromString(String yaml) {
        return loadYamlFromString(yaml, System.getProperties());
    }

    /**
     * Creates a JetConfig from the provided YAML string and properties to resolve
     * the variables in the YAML.
     *
     * @param yaml the YAML content as a Hazelcast Jet YAML String
     * @throws IllegalArgumentException      if the YAML is null or empty or if
     *                                       properties is null
     * @throws InvalidConfigurationException if the YAML content is invalid
     */
    @Nonnull
    public static JetConfig loadYamlFromString(String yaml, Properties properties) {
        LOGGER.info("Configuring Hazelcast Jet from 'in-memory yaml'.");
        if (isNullOrEmptyAfterTrim(yaml)) {
            throw new IllegalArgumentException("YAML configuration is null or empty! Please use a well-structured YAML.");
        }
        checkTrue(properties != null, "properties can't be null");
        InputStream in = new ByteArrayInputStream(stringToBytes(yaml));
        return new YamlJetConfigBuilder(in).setProperties(properties).build();
    }

    /**
     * Uses the thread's context class loader to load JetConfig from the
     * classpath resource named by the argument. Uses {@code
     * System.getProperties()} to resolve the variables in the XML or Yaml.
     * <p>
     * This method loads the nested {@linkplain #getHazelcastConfig() Hazelcast
     * config} using the built-in {@link #loadDefault lookup mechanism}, but
     * you can replace it afterwards by calling {@link #setHazelcastConfig
     * setHazelcastConfig()} with, for example, {@link
     * com.hazelcast.config.ClasspathXmlConfig ClasspathXmlConfig} or {@link
     * com.hazelcast.config.FileSystemXmlConfig FileSystemXmlConfig}.
     *
     * @param resource names the classpath resource containing the XML or Yaml configuration file
     * @throws com.hazelcast.core.HazelcastException if the XML or Yaml content is invalid
     * @throws IllegalArgumentException              if classpath resource is not found
     * @deprecated see {@linkplain #loadFromClasspath(ClassLoader,String)}}
     */
    @Nonnull
    @Deprecated
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
     *                 classpath
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     * @throws IllegalArgumentException              if classpath resource is not found
     * @deprecated see {@linkplain #loadFromClasspath(ClassLoader, String, Properties)}}
     */
    @Nonnull
    @Deprecated
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
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     * @deprecated see {@linkplain #loadXmlFromStream(InputStream)}  and
     * {@linkplain #loadYamlFromStream(InputStream)}
     */
    @Nonnull
    @Deprecated
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
     * @param properties   the properties to resolve variables in the XML
     * @throws com.hazelcast.core.HazelcastException if the XML content is invalid
     * @deprecated see {@linkplain #loadXmlFromStream(InputStream, Properties)}  and
     * {@linkplain #loadYamlFromStream(InputStream, Properties)}
     */
    @Nonnull
    @Deprecated
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
        config.getHotRestartPersistenceConfig().setBaseDir(new File(jetHome(), "recovery").getAbsoluteFile());
        return config;
    }

    /**
     * Returns the absolute path for jet.home based from the system property
     * {@link com.hazelcast.jet.impl.util.JetProperties#JET_HOME}
     */
    private static String jetHome() {
        return new File(System.getProperty(JET_HOME.getName(), JET_HOME.getDefaultValue())).getAbsolutePath();
    }
}
