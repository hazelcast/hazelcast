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

import com.hazelcast.merge.AddNewEntryMergePolicy;
import com.hazelcast.merge.HigherHitsMergePolicy;
import com.hazelcast.merge.LatestUpdateMergePolicy;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.IOUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Config implements DataSerializable {

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

    private Properties properties = new Properties();

    private Map<String, MergePolicyConfig> mapMergePolicyConfigs = new ConcurrentHashMap<String, MergePolicyConfig>();

    public Config() {
        final String superClientProp = System.getProperty("hazelcast.super.client");
        if ("true".equalsIgnoreCase(superClientProp)) {
            superClient = true;
        }
        String os = System.getProperty("os.name").toLowerCase();
        reuseAddress = (os.indexOf("win") == -1);
        addMergePolicyConfig(new MergePolicyConfig(AddNewEntryMergePolicy.NAME, new AddNewEntryMergePolicy()));
        addMergePolicyConfig(new MergePolicyConfig(HigherHitsMergePolicy.NAME, new HigherHitsMergePolicy()));
        addMergePolicyConfig(new MergePolicyConfig(LatestUpdateMergePolicy.NAME, new LatestUpdateMergePolicy()));
    }

    public void addMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        mapMergePolicyConfigs.put(mergePolicyConfig.getName(), mergePolicyConfig);
    }

    public MergePolicyConfig getMergePolicyConfig(String name) {
        return mapMergePolicyConfigs.get(name);
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public Config setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
        return this;
    }

    public Config setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    public Properties getProperties() {
        return properties;
    }

    public String getProperty(String name) {
        return properties.getProperty(name);
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
            defaultConfig.setName("default");
            mapQueueConfigs.put("default", defaultConfig);
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
            defaultConfig.setName("default");
            mapConfigs.put("default", defaultConfig);
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
            defaultConfig.setName("default");
            mapTopicConfigs.put("default", defaultConfig);
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

    public Map<String, ExecutorConfig> getExecutorConfigMap() {
        return mapExecutors;
    }

    public void setExecutorConfigMap(Map<String, ExecutorConfig> mapExecutors) {
        this.mapExecutors = mapExecutors;
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
    public void setQConfigs(Map<String, QueueConfig> mapQConfigs) {
        this.mapQueueConfigs = mapQConfigs;
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
    
    public void checkCompatibility(final Config config){
        if (config == null){
            throw new IllegalArgumentException("Expected not null config");
        }
        if (!this.groupConfig.equals(config.getGroupConfig())){
            throw new RuntimeException("Incompatible group config:" + config.getGroupConfig());
        }
        checkMapConfigCompatible(config);
        checkQueueConfigCompatible(config);
        checkTopicConfigCompatible(config);
    }

    private void checkMapConfigCompatible(final Config config) {
        Set<String> mapConfigNames = new HashSet<String>(mapConfigs.keySet());
        mapConfigNames.addAll(config.mapConfigs.keySet());
        for (final String name : mapConfigNames) {
            final MapConfig thisMapConfig = getMapConfig(name);
            final MapConfig thatMapConfig = config.getMapConfig(name);
            if (!thisMapConfig.isCompatible(thatMapConfig)) {
                throw new RuntimeException("Incompatible map config:" + thatMapConfig);
            }
        }
    }
    
    private void checkQueueConfigCompatible(final Config config) {
        Set<String> queueConfigNames = new HashSet<String>(mapQueueConfigs.keySet());
        queueConfigNames.addAll(config.mapQueueConfigs.keySet());
        for (final String name : queueConfigNames) {
            final QueueConfig thisQueueConfig = getQueueConfig(name);
            final QueueConfig thatQueueConfig = config.getQueueConfig(name);
            if (!thisQueueConfig.isCompatible(thatQueueConfig)) {
                throw new RuntimeException("Incompatible queue config:" + thatQueueConfig);
            }
        }
    }
    
    private void checkTopicConfigCompatible(final Config config) {
        Set<String> topicConfigNames = new HashSet<String>(mapTopicConfigs.keySet());
        topicConfigNames.addAll(config.mapTopicConfigs.keySet());
        for (final String name : topicConfigNames) {
            final TopicConfig thisTopicConfig = getTopicConfig(name);
            final TopicConfig thatTopicConfig = config.getTopicConfig(name);
            if (!thisTopicConfig.equals(thatTopicConfig)) {
                throw new RuntimeException("Incompatible topic config:" + thatTopicConfig);
            }
        }
    }
    
    public void readData(DataInput in) throws IOException {
        groupConfig = new GroupConfig();
        groupConfig.readData(in);
        port = in.readInt();
        boolean[] b1 = IOUtil.fromByte(in.readByte());
        reuseAddress = b1[0];
        portAutoIncrement = b1[1];
        superClient = b1[2];
        
        boolean[] b2 = IOUtil.fromByte(in.readByte());
        
        boolean hasMapConfigs = b2[0];
        boolean hasMapExecutors = b2[1];
        boolean hasMapTopicConfigs = b2[2];
        boolean hasMapQueueConfigs = b2[3];
        boolean hasMapMergePolicyConfigs = b2[4];
        boolean hasProperties = b2[5];
        
        networkConfig = new NetworkConfig();
        networkConfig.readData(in);
        
        executorConfig = new ExecutorConfig();
        executorConfig.readData(in);
        
        if (hasMapConfigs){
            int size = in.readInt();
            mapConfigs = new ConcurrentHashMap<String, MapConfig>(size);
            for(int i = 0; i < size; i++){
                final MapConfig mapConfig = new MapConfig();
                mapConfig.readData(in);
                mapConfigs.put(mapConfig.getName(), mapConfig);
            }
        }
        if (hasMapExecutors){
            int size = in.readInt();
            mapExecutors = new ConcurrentHashMap<String, ExecutorConfig>(size);
            for(int i = 0; i < size; i++){
                final ExecutorConfig executorConfig = new ExecutorConfig();
                executorConfig.readData(in);
                mapExecutors.put(executorConfig.getName(), executorConfig);
            }
        }
        if (hasMapTopicConfigs){
            int size = in.readInt();
            mapTopicConfigs = new ConcurrentHashMap<String, TopicConfig>(size);
            for(int i = 0; i < size; i++){
                final TopicConfig topicConfig = new TopicConfig();
                topicConfig.readData(in);
                mapTopicConfigs.put(topicConfig.getName(), topicConfig);
            }
        }
        if (hasMapQueueConfigs){
            int size = in.readInt();
            mapQueueConfigs = new ConcurrentHashMap<String, QueueConfig>(size);
            for(int i = 0; i < size; i++){
                final QueueConfig queueConfig = new QueueConfig();
                queueConfig.readData(in);
                mapQueueConfigs.put(queueConfig.getName(), queueConfig);
            }
        }
        if (hasMapMergePolicyConfigs){
            // TODO: Map<String, MergePolicyConfig> mapMergePolicyConfigs
        }
        if (hasProperties){
            int size = in.readInt();
            properties = new Properties();
            for(int i = 0; i < size; i++){
                final String name = in.readUTF();
                final String value = in.readUTF();
                properties.put(name, value);
            }
        }
    }
    
    public void writeData(DataOutput out) throws IOException {
        getGroupConfig().writeData(out);
        out.writeInt(port);
        boolean hasMapConfigs = mapConfigs != null && !mapConfigs.isEmpty();
        boolean hasMapExecutors = mapExecutors != null && !mapExecutors.isEmpty();
        boolean hasMapTopicConfigs = mapTopicConfigs != null && !mapTopicConfigs.isEmpty();
        boolean hasMapQueueConfigs = mapQueueConfigs != null && !mapQueueConfigs.isEmpty();
        boolean hasMapMergePolicyConfigs = mapMergePolicyConfigs != null && !mapMergePolicyConfigs.isEmpty();
        boolean hasProperties = properties != null && !properties.isEmpty();
        
        out.writeByte(IOUtil.toByte(reuseAddress, 
            portAutoIncrement,
            superClient));
        
        out.writeByte(IOUtil.toByte(
            hasMapConfigs,
            hasMapExecutors,
            hasMapTopicConfigs,
            hasMapQueueConfigs,
            hasMapMergePolicyConfigs,
            hasProperties));
        
        networkConfig.writeData(out);
        executorConfig.writeData(out);
        
        if (hasMapConfigs){
            out.writeInt(mapConfigs.size());
            for (final Entry<String, MapConfig> entry : mapConfigs.entrySet()) {
                final String name = entry.getKey();
                final MapConfig mapConfig = entry.getValue();
                mapConfig.setName(name);
                mapConfig.writeData(out);
            }
        }
        if (hasMapExecutors){
            out.writeInt(mapExecutors.size());
            for (final Entry<String, ExecutorConfig> entry : mapExecutors.entrySet()) {
                final String name = entry.getKey();
                final ExecutorConfig executorConfig = entry.getValue();
                executorConfig.setName(name);
                executorConfig.writeData(out);
            }
        }
        if (hasMapTopicConfigs){
            out.writeInt(mapTopicConfigs.size());
            for (final Entry<String, TopicConfig> entry : mapTopicConfigs.entrySet()) {
                final String name = entry.getKey();
                final TopicConfig topicConfig = entry.getValue();
                topicConfig.setName(name);
                topicConfig.writeData(out);
            }
        }
        if (hasMapQueueConfigs){
            out.writeInt(mapQueueConfigs.size());
            for (final Entry<String, QueueConfig> entry : mapQueueConfigs.entrySet()) {
                final String name = entry.getKey();
                final QueueConfig queueConfig = entry.getValue();
                queueConfig.setName(name);
                queueConfig.writeData(out);
            }
        }
        if (hasMapMergePolicyConfigs){
            // TODO: Map<String, MergePolicyConfig> mapMergePolicyConfigs
        }
        if (hasProperties){
            out.writeInt(properties.size());
            for (final Entry<Object, Object> entry : properties.entrySet()) {
                final String key = (String) entry.getKey();
                final String value = (String) entry.getValue();
                out.writeUTF(key);
                out.writeUTF(value);
            }
        }
    }

    @Override
    public String toString() {
        return "Config [groupConfig=" + this.groupConfig 
            + ", port=" + this.port 
            + ", superClient=" + this.superClient
            + ", reuseAddress=" + this.reuseAddress
            + ", portAutoIncrement=" + this.portAutoIncrement
            + ", properties=" + this.properties 
            + ", networkConfig=" + this.networkConfig 
            + ", mapConfigs=" + this.mapConfigs
            + ", mapMergePolicyConfigs=" + this.mapMergePolicyConfigs
            + ", executorConfig=" + this.executorConfig 
            + ", mapExecutors=" + this.mapExecutors 
            + ", mapTopicConfigs=" + this.mapTopicConfigs 
            + ", mapQueueConfigs=" + this.mapQueueConfigs 
            + "]";
    }
    
    
}
