/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.config;

import java.io.File;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Config {

    public static final int DEFAULT_PORT = 5701;

    private String xmlConfig = null;

    private GroupConfig groupConfig = new GroupConfig();

    private int port = DEFAULT_PORT;

    private boolean reuseAddress = false;

    private boolean portAutoIncrement = true;

    private ExecutorConfig executorConfig = new ExecutorConfig();

    private Map<String, ExecutorConfig> mapExecutors = new ConcurrentHashMap<String, ExecutorConfig>();

    private Map<String, TopicConfig> mapTopicConfigs = new ConcurrentHashMap<String, TopicConfig>();

    private Map<String, QueueConfig> mapQueueConfigs = new ConcurrentHashMap<String, QueueConfig>();

    private Map<String, MapConfig> mapConfigs = new ConcurrentHashMap<String, MapConfig>();

    private URL configurationUrl;

    private File configurationFile;

    private NetworkConfig networkConfig = new NetworkConfig();

    private boolean superClient = false;

    private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    private Map<String, String> properties = new HashMap<String, String>();

    public Config() {
        final String superClientProp = System.getProperty("hazelcast.super.client");
        if ("true".equalsIgnoreCase(superClientProp)) {
            superClient = true;
        }
        String os = System.getProperty("os.name").toLowerCase();
        reuseAddress = (os.indexOf("win") == -1);
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public Config setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
        return this;
    }

    public void setProperty(String name, String value) {
        properties.put(name, value);
    }

    public String getProperty(String name) {
        return properties.get(name);
    }

    public QueueConfig getQueueConfig(final String name) {
        final Set<String> qNames = mapQueueConfigs.keySet();
        for (final String pattern : qNames) {
            if (nameMatches(name, pattern)) {
                return mapQueueConfigs.get(pattern);
            }
        }
        QueueConfig defaultConfig = mapQueueConfigs.get("default");
        if (defaultConfig == null) {
            defaultConfig = new QueueConfig();
        }
        return defaultConfig;
    }

    public MapConfig getMapConfig(final String name) {
        final Set<String> qNames = mapConfigs.keySet();
        for (final String pattern : qNames) {
            if (nameMatches(name, pattern)) {
                return mapConfigs.get(pattern);
            }
        }
        MapConfig defaultConfig = mapConfigs.get("default");
        if (defaultConfig == null) {
            defaultConfig = new MapConfig();
        }
        return defaultConfig;
    }

    public TopicConfig getTopicConfig(final String name) {
        final Set<String> tNames = mapTopicConfigs.keySet();
        for (final String pattern : tNames) {
            if (nameMatches(name, pattern)) {
                return mapTopicConfigs.get(pattern);
            }
        }
        TopicConfig defaultConfig = mapTopicConfigs.get("default");
        if (defaultConfig == null) {
            defaultConfig = new TopicConfig();
        }
        return defaultConfig;
    }

    private boolean nameMatches(final String name, final String pattern) {
        final int index = pattern.indexOf('*');
        if (index == -1) {
            return name.equals(pattern);
        } else {
            final String firstPart = pattern.substring(0, index);
            final int indexFirstPart = name.indexOf(firstPart, 0);
            if (indexFirstPart == -1) {
                return false;
            }
            final String secondPart = pattern.substring(index + 1);
            final int indexSecondPart = name.indexOf(secondPart, index + 1);
            return indexSecondPart != -1;
        }
    }

    public NetworkConfig getNetworkConfig() {
        return networkConfig;
    }

    public Config setNetworkConfig(NetworkConfig networkConfig) {
        this.networkConfig = networkConfig;
        return this;
    }

    /**
     * @return the xmlConfig
     */
    public String getXmlConfig() {
        return xmlConfig;
    }

    /**
     * @param xmlConfig the xmlConfig to set
     */
    public Config setXmlConfig(String xmlConfig) {
        this.xmlConfig = xmlConfig;
        return this;
    }

    public GroupConfig getGroupConfig() {
        return groupConfig;
    }

    public Config setGroupConfig(final GroupConfig groupConfig) {
        this.groupConfig = groupConfig;
        return this;
    }

    /**
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * @param port the port to set
     */
    public Config setPort(int port) {
        this.port = port;
        return this;
    }

    /**
     * @return the portAutoIncrement
     */
    public boolean isPortAutoIncrement() {
        return portAutoIncrement;
    }

    /**
     * @param portAutoIncrement the portAutoIncrement to set
     */
    public Config setPortAutoIncrement(boolean portAutoIncrement) {
        this.portAutoIncrement = portAutoIncrement;
        return this;
    }

    public boolean isReuseAddress() {
        return reuseAddress;
    }

    public Config setReuseAddress(boolean reuseAddress) {
        this.reuseAddress = reuseAddress;
        return this;
    }

    /**
     * @return the executorConfig
     * @deprecated use getExecutorConfig (name) instead
     */
    public ExecutorConfig getExecutorConfig() {
        return executorConfig;
    }

    /**
     * @param executorConfig the executorConfig to set
     * @deprecated use addExecutorConfig instead
     */
    public Config setExecutorConfig(ExecutorConfig executorConfig) {
        addExecutorConfig(executorConfig);
        return this;
    }

    /**
     * Adds a new ExecutorConfig by name
     *
     * @param executorConfig executor config to add
     * @return this config instance
     */
    public Config addExecutorConfig(ExecutorConfig executorConfig) {
        this.mapExecutors.put(executorConfig.getName(), executorConfig);
        return this;
    }

    /**
     * Returns the ExecutorConfig for the given name
     *
     * @param name name of the executor config
     * @return ExecutorConfig
     */
    public ExecutorConfig getExecutorConfig(String name) {
        ExecutorConfig ec = this.mapExecutors.get(name);
        if (ec == null) {
            ExecutorConfig defaultConfig = mapExecutors.get("default");
            if (defaultConfig != null) {
                ec = new ExecutorConfig(name,
                        defaultConfig.getCorePoolSize(),
                        defaultConfig.getMaxPoolSize(),
                        defaultConfig.getKeepAliveSeconds());
            }
        }
        if (ec == null) {
            ec = new ExecutorConfig(name);
            mapExecutors.put(name, ec);
        }
        return ec;
    }

    /**
     * Returns the collection of executor configs.
     *
     * @return collection of executor configs.
     */
    public Collection<ExecutorConfig> getExecutorConfigs() {
        return mapExecutors.values();
    }

    /**
     * @return the mapTopicConfigs
     */
    public Map<String, TopicConfig> getTopicConfigs() {
        return mapTopicConfigs;
    }

    /**
     * @param mapTopicConfigs the mapTopicConfigs to set
     */
    public Config setTopicConfigs(Map<String, TopicConfig> mapTopicConfigs) {
        this.mapTopicConfigs = mapTopicConfigs;
        return this;
    }

    /**
     * @return the mapQConfigs
     */
    public Map<String, QueueConfig> getQConfigs() {
        return mapQueueConfigs;
    }

    /**
     * @param mapQConfigs the mapQConfigs to set
     */
    public Config setMapQConfigs(Map<String, QueueConfig> mapQConfigs) {
        this.mapQueueConfigs = mapQConfigs;
        return this;
    }

    /**
     * @return the mapConfigs
     */
    public Map<String, MapConfig> getMapConfigs() {
        return mapConfigs;
    }

    /**
     * @param mapConfigs the mapConfigs to set
     */
    public Config setMapConfigs(Map<String, MapConfig> mapConfigs) {
        this.mapConfigs = mapConfigs;
        return this;
    }

    /**
     * @return the configurationUrl
     */
    public URL getConfigurationUrl() {
        return configurationUrl;
    }

    /**
     * @param configurationUrl the configurationUrl to set
     */
    public Config setConfigurationUrl(URL configurationUrl) {
        this.configurationUrl = configurationUrl;
        return this;
    }

    /**
     * @return the configurationFile
     */
    public File getConfigurationFile() {
        return configurationFile;
    }

    /**
     * @param configurationFile the configurationFile to set
     */
    public Config setConfigurationFile(File configurationFile) {
        this.configurationFile = configurationFile;
        return this;
    }

    public boolean isSuperClient() {
        return superClient;
    }

    public Config setSuperClient(boolean superClient) {
        this.superClient = superClient;
        return this;
    }
}
