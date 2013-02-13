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

package com.hazelcast.config;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.ByteUtil;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import static java.text.MessageFormat.format;

public class Config implements DataSerializable {

    private URL configurationUrl;

    private File configurationFile;

    private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    private Properties properties = new Properties();

    private String instanceName = null;

    private GroupConfig groupConfig = new GroupConfig();

    private boolean checkCompatibility = true;

    private NetworkConfig networkConfig = new NetworkConfig();

    private Map<String, MapConfig> mapConfigs = new ConcurrentHashMap<String, MapConfig>();

    private Map<String, TopicConfig> topicConfigs = new ConcurrentHashMap<String, TopicConfig>();

    private Map<String, QueueConfig> queueConfigs = new ConcurrentHashMap<String, QueueConfig>();

    private Map<String, MultiMapConfig> multiMapConfigs = new ConcurrentHashMap<String, MultiMapConfig>();

    private Map<String, ExecutorConfig> executorConfigs = new ConcurrentHashMap<String, ExecutorConfig>();

    private Map<String, SemaphoreConfig> semaphoreConfigs = new ConcurrentHashMap<String, SemaphoreConfig>();

    private Map<String, WanReplicationConfig> wanReplicationConfigs = new ConcurrentHashMap<String, WanReplicationConfig>();

    private ServicesConfig servicesConfigConfig = new ServicesConfig();

    private SecurityConfig securityConfig = new SecurityConfig();

    private List<ListenerConfig> listenerConfigs = new LinkedList<ListenerConfig>();

    private PartitionGroupConfig partitionGroupConfig = new PartitionGroupConfig();

    private ManagementCenterConfig managementCenterConfig = new ManagementCenterConfig();

    private SerializationConfig serializationConfig = new SerializationConfig();

    private ManagedContext managedContext;

    private String licenseKey;

    public Config() {
    }

    public WanReplicationConfig getWanReplicationConfig(String name) {
        return wanReplicationConfigs.get(name);
    }

    public Config addWanReplicationConfig(WanReplicationConfig wanReplicationConfig) {
        wanReplicationConfigs.put(wanReplicationConfig.getName(), wanReplicationConfig);
        return this;
    }

    public Map<String, WanReplicationConfig> getWanReplicationConfigs() {
        return wanReplicationConfigs;
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

    public Properties getProperties() {
        return properties;
    }

    public String getProperty(String name) {
        String value = properties.getProperty(name);
        return value != null ? value : System.getProperty(name);
    }

    public QueueConfig getQueueConfig(final String name) {
        QueueConfig config;
        if ((config = lookupByPattern(queueConfigs, name)) != null) return config;
        QueueConfig defConfig = queueConfigs.get("default");
        if (defConfig == null) {
            defConfig = new QueueConfig();
            defConfig.setName("default");
            addQueueConfig(defConfig);
        }
        config = new QueueConfig(defConfig);
        config.setName(name);
        addQueueConfig(config);
        return config;
    }

    public MapConfig getMapConfig(final String name) {
        MapConfig config;
        if ((config = lookupByPattern(mapConfigs, name)) != null) return config;
        MapConfig defConfig = mapConfigs.get("default");
        if (defConfig == null) {
            defConfig = new MapConfig();
            defConfig.setName("default");
            addMapConfig(defConfig);
        }
        config = new MapConfig(defConfig);
        config.setName(name);
        addMapConfig(config);
        return config;
    }

    public MultiMapConfig getMultiMapConfig(final String name) {
        MultiMapConfig config;
        if ((config = lookupByPattern(multiMapConfigs, name)) != null) return config;
        MultiMapConfig defConfig = multiMapConfigs.get("default");
        if (defConfig == null) {
            defConfig = new MultiMapConfig();
            defConfig.setName("default");
            addMultiMapConfig(defConfig);
        }
        config = new MultiMapConfig(defConfig);
        config.setName(name);
        addMultiMapConfig(config);
        return config;
    }

    public TopicConfig getTopicConfig(final String name) {
        TopicConfig config;
        if ((config = lookupByPattern(topicConfigs, name)) != null) {
            return config;
        }
        TopicConfig defConfig = topicConfigs.get("default");
        if (defConfig == null) {
            defConfig = new TopicConfig();
            defConfig.setName("default");
            addTopicConfig(defConfig);
        }
        config = new TopicConfig(defConfig);
        config.setName(name);
        addTopicConfig(config);
        return config;
    }

    private static <T> T lookupByPattern(Map<String, T> map, String name) {
        T t = map.get(name);
        if (t == null) {
            final Set<String> tNames = map.keySet();
            for (final String pattern : tNames) {
                if (nameMatches(name, pattern)) {
                    return map.get(pattern);
                }
            }
        }
        return t;
    }

    public static boolean nameMatches(final String name, final String pattern) {
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

    public GroupConfig getGroupConfig() {
        return groupConfig;
    }

    public boolean isCheckCompatibility() {
        return this.checkCompatibility;
    }

    public Config setCheckCompatibility(boolean checkCompatibility) {
        this.checkCompatibility = checkCompatibility;
        return this;
    }

    /**
     * Adds a new ExecutorConfig by name
     *
     * @param executorConfig executor config to add
     * @return this config instance
     */
    public Config addExecutorConfig(ExecutorConfig executorConfig) {
        this.executorConfigs.put(executorConfig.getName(), executorConfig);
        return this;
    }

    /**
     * Returns the ExecutorConfig for the given name
     *
     * @param name name of the executor config
     * @return ExecutorConfig
     */
    public ExecutorConfig getExecutorConfig(String name) {
        ExecutorConfig ec = lookupByPattern(executorConfigs, name);
        if (ec != null) {
            return ec;
        }

        ExecutorConfig defaultConfig = executorConfigs.get("default");
        if (defaultConfig != null) {
            ec = new ExecutorConfig(name,
                    defaultConfig.getPoolSize());
        }
        if (ec == null) {
            ec = new ExecutorConfig(name);
            executorConfigs.put(name, ec);
        }
        return ec;
    }

    /**
     * Returns the collection of executor configs.
     *
     * @return collection of executor configs.
     */
    public Collection<ExecutorConfig> getExecutorConfigs() {
        return executorConfigs.values();
    }

    public Map<String, ExecutorConfig> getExecutorConfigMap() {
        return Collections.unmodifiableMap(executorConfigs);
    }

    public Config addTopicConfig(TopicConfig topicConfig) {
        topicConfigs.put(topicConfig.getName(), topicConfig);
        return this;
    }

    /**
     * @return the topicConfigs
     */
    public Map<String, TopicConfig> getTopicConfigs() {
        return Collections.unmodifiableMap(topicConfigs);
    }

    /**
     * @return the mapQConfigs
     */
    public Map<String, QueueConfig> getQConfigs() {
        return Collections.unmodifiableMap(queueConfigs);
    }

    public Config addQueueConfig(QueueConfig queueConfig) {
        queueConfigs.put(queueConfig.getName(), queueConfig);
        return this;
    }

    public Config addMapConfig(MapConfig mapConfig) {
        mapConfigs.put(mapConfig.getName(), mapConfig);
        return this;
    }

    public Config addMultiMapConfig(MultiMapConfig multiMapConfig) {
        multiMapConfigs.put(multiMapConfig.getName(), multiMapConfig);
        return this;
    }

    /**
     * @return the mapConfigs
     */
    public Map<String, MapConfig> getMapConfigs() {
        return Collections.unmodifiableMap(mapConfigs);
    }

    public Map<String, MultiMapConfig> getMultiMapConfigs() {
        return Collections.unmodifiableMap(multiMapConfigs);
    }

    /**
     * Adds a new SemaphoreConfig by name
     *
     * @param semaphoreConfig semaphore config to add
     * @return this config instance
     */
    public Config addSemaphoreConfig(SemaphoreConfig semaphoreConfig) {
        this.semaphoreConfigs.put(semaphoreConfig.getName(), semaphoreConfig);
        return this;
    }

    /**
     * Returns the SemaphoreConfig for the given name
     *
     * @param name name of the semaphore config
     * @return SemaphoreConfig
     */
    public SemaphoreConfig getSemaphoreConfig(String name) {
        SemaphoreConfig sc = lookupByPattern(semaphoreConfigs, name);
        if (sc != null) {
            return sc;
        }

        SemaphoreConfig defaultConfig = semaphoreConfigs.get("default");
        if (defaultConfig == null) {
            defaultConfig = new SemaphoreConfig();
            defaultConfig.setName("default");
            addSemaphoreConfig(defaultConfig);
        }
        sc = new SemaphoreConfig(defaultConfig);
        addSemaphoreConfig(sc);
        return sc;
    }

    /**
     * Returns the collection of semaphore configs.
     *
     * @return collection of semaphore configs.
     */
    public Collection<SemaphoreConfig> getSemaphoreConfigs() {
        return semaphoreConfigs.values();
    }

    public Map<String, SemaphoreConfig> getSemaphoreConfigMap() {
        return Collections.unmodifiableMap(semaphoreConfigs);
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

    public SecurityConfig getSecurityConfig() {
        return securityConfig;
    }

    public Config addListenerConfig(ListenerConfig listenerConfig) {
        getListenerConfigs().add(listenerConfig);
        return this;
    }

    public List<ListenerConfig> getListenerConfigs() {
        return listenerConfigs;
    }

    public SerializationConfig getSerializationConfig() {
        return serializationConfig;
    }

    public PartitionGroupConfig getPartitionGroupConfig() {
        return partitionGroupConfig;
    }

    public ManagedContext getManagedContext() {
        return managedContext;
    }

    public Config setManagedContext(final ManagedContext managedContext) {
        this.managedContext = managedContext;
        return this;
    }

    /**
     * @param config
     * @return true if config is compatible with this one,
     *         false if config belongs to another group
     * @throws RuntimeException if map, queue, topic configs are incompatible
     */
    public boolean isCompatible(final Config config) {
        if (config == null) {
            throw new IllegalArgumentException("Expected not null config");
        }
        if (!this.groupConfig.getName().equals(config.getGroupConfig().getName())) {
            return false;
        }
        if (!this.groupConfig.getPassword().equals(config.getGroupConfig().getPassword())) {
            throw new HazelcastException("Incompatible group password");
        }
        if (checkCompatibility) {
            checkMapConfigCompatible(config);
            checkQueueConfigCompatible(config);
            checkTopicConfigCompatible(config);
        }
        return true;
    }

    private void checkMapConfigCompatible(final Config config) {
        Set<String> mapConfigNames = new HashSet<String>(mapConfigs.keySet());
        mapConfigNames.addAll(config.mapConfigs.keySet());
        for (final String name : mapConfigNames) {
            final MapConfig thisMapConfig = lookupByPattern(mapConfigs, name);
            final MapConfig thatMapConfig = lookupByPattern(config.mapConfigs, name);
            if (thisMapConfig != null && thatMapConfig != null &&
                    !thisMapConfig.isCompatible(thatMapConfig)) {
                throw new HazelcastException(format("Incompatible map config this:\n{0}\nanother:\n{1}",
                        thisMapConfig, thatMapConfig));
            }
        }
    }

    private void checkQueueConfigCompatible(final Config config) {
        Set<String> queueConfigNames = new HashSet<String>(queueConfigs.keySet());
        queueConfigNames.addAll(config.queueConfigs.keySet());
        for (final String name : queueConfigNames) {
            final QueueConfig thisQueueConfig = lookupByPattern(queueConfigs, name);
            final QueueConfig thatQueueConfig = lookupByPattern(config.queueConfigs, name);
            if (thisQueueConfig != null && thatQueueConfig != null &&
                    !thisQueueConfig.isCompatible(thatQueueConfig)) {
                throw new HazelcastException(format("Incompatible queue config this:\n{0}\nanother:\n{1}",
                        thisQueueConfig, thatQueueConfig));
            }
        }
    }

    private void checkTopicConfigCompatible(final Config config) {
        Set<String> topicConfigNames = new HashSet<String>(topicConfigs.keySet());
        topicConfigNames.addAll(config.topicConfigs.keySet());
        for (final String name : topicConfigNames) {
            final TopicConfig thisTopicConfig = lookupByPattern(topicConfigs, name);
            final TopicConfig thatTopicConfig = lookupByPattern(config.topicConfigs, name);
            if (thisTopicConfig != null && thatTopicConfig != null &&
                    !thisTopicConfig.equals(thatTopicConfig)) {
                throw new HazelcastException(format("Incompatible topic config this:\n{0}\nanother:\n{1}",
                        thisTopicConfig, thatTopicConfig));
            }
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        groupConfig = new GroupConfig();
        groupConfig.readData(in);
        boolean[] b1 = ByteUtil.fromByte(in.readByte());
        checkCompatibility = b1[0];
        boolean[] b2 = ByteUtil.fromByte(in.readByte());
        boolean hasMapConfigs = b2[0];
        boolean hasExecutors = b2[1];
        boolean hasTopicConfigs = b2[2];
        boolean hasQueueConfigs = b2[3];
        boolean hasSemaphoreConfigs = b2[4];
        boolean hasProperties = b2[5];
        networkConfig = new NetworkConfig();
        networkConfig.readData(in);
        if (hasMapConfigs) {
            int size = in.readInt();
            mapConfigs = new ConcurrentHashMap<String, MapConfig>(size);
            for (int i = 0; i < size; i++) {
                final MapConfig mapConfig = new MapConfig();
                mapConfig.readData(in);
                mapConfigs.put(mapConfig.getName(), mapConfig);
            }
        }
        if (hasExecutors) {
            int size = in.readInt();
            executorConfigs = new ConcurrentHashMap<String, ExecutorConfig>(size);
            for (int i = 0; i < size; i++) {
                final ExecutorConfig executorConfig = new ExecutorConfig();
                executorConfig.readData(in);
                executorConfigs.put(executorConfig.getName(), executorConfig);
            }
        }
        if (hasTopicConfigs) {
            int size = in.readInt();
            topicConfigs = new ConcurrentHashMap<String, TopicConfig>(size);
            for (int i = 0; i < size; i++) {
                final TopicConfig topicConfig = new TopicConfig();
                topicConfig.readData(in);
                topicConfigs.put(topicConfig.getName(), topicConfig);
            }
        }
        if (hasQueueConfigs) {
            int size = in.readInt();
            queueConfigs = new ConcurrentHashMap<String, QueueConfig>(size);
            for (int i = 0; i < size; i++) {
                final QueueConfig queueConfig = new QueueConfig();
                queueConfig.readData(in);
                queueConfigs.put(queueConfig.getName(), queueConfig);
            }
        }
        if (hasSemaphoreConfigs) {
            int size = in.readInt();
            semaphoreConfigs = new ConcurrentHashMap<String, SemaphoreConfig>(size);
            for (int i = 0; i < size; i++) {
                final SemaphoreConfig semaphoreConfig = new SemaphoreConfig();
                semaphoreConfig.readData(in);
                semaphoreConfigs.put(semaphoreConfig.getName(), semaphoreConfig);
            }
        }
        if (hasProperties) {
            int size = in.readInt();
            properties = new Properties();
            for (int i = 0; i < size; i++) {
                final String name = in.readUTF();
                final String value = in.readUTF();
                properties.put(name, value);
            }
        }
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        getGroupConfig().writeData(out);
        boolean hasMapConfigs = mapConfigs != null && !mapConfigs.isEmpty();
        boolean hasExecutors = executorConfigs != null && !executorConfigs.isEmpty();
        boolean hasTopicConfigs = topicConfigs != null && !topicConfigs.isEmpty();
        boolean hasQueueConfigs = queueConfigs != null && !queueConfigs.isEmpty();
        boolean hasSemaphoreConfigs = semaphoreConfigs != null && !semaphoreConfigs.isEmpty();
        boolean hasProperties = properties != null && !properties.isEmpty();
        out.writeByte(ByteUtil.toByte(checkCompatibility));
        out.writeByte(ByteUtil.toByte(
                hasMapConfigs,
                hasExecutors,
                hasTopicConfigs,
                hasQueueConfigs,
                hasSemaphoreConfigs,
                hasProperties));
        networkConfig.writeData(out);
        if (hasMapConfigs) {
            out.writeInt(mapConfigs.size());
            for (final Entry<String, MapConfig> entry : mapConfigs.entrySet()) {
                final String name = entry.getKey();
                final MapConfig mapConfig = entry.getValue();
                mapConfig.setName(name);
                mapConfig.writeData(out);
            }
        }
        if (hasExecutors) {
            out.writeInt(executorConfigs.size());
            for (final Entry<String, ExecutorConfig> entry : executorConfigs.entrySet()) {
                final String name = entry.getKey();
                final ExecutorConfig executorConfig = entry.getValue();
                executorConfig.setName(name);
                executorConfig.writeData(out);
            }
        }
        if (hasQueueConfigs) {
            out.writeInt(queueConfigs.size());
            for (final Entry<String, QueueConfig> entry : queueConfigs.entrySet()) {
                final String name = entry.getKey();
                final QueueConfig queueConfig = entry.getValue();
                queueConfig.setName(name);
                queueConfig.writeData(out);
            }
        }
        if (hasTopicConfigs) {
            out.writeInt(topicConfigs.size());
            for (final Entry<String, TopicConfig> entry : topicConfigs.entrySet()) {
                final String name = entry.getKey();
                final TopicConfig topicConfig = entry.getValue();
                topicConfig.setName(name);
                topicConfig.writeData(out);
            }
        }
        if (hasProperties) {
            out.writeInt(properties.size());
            for (final Entry<Object, Object> entry : properties.entrySet()) {
                final String key = (String) entry.getKey();
                final String value = (String) entry.getValue();
                out.writeUTF(key);
                out.writeUTF(value);
            }
        }
    }

    public String getInstanceName() {
        return instanceName;
    }

    public Config setInstanceName(String instanceName) {
        this.instanceName = instanceName;
        return this;
    }

    public String getLicenseKey() {
        return licenseKey;
    }

    public Config setLicenseKey(final String licenseKey) {
        this.licenseKey = licenseKey;
        return this;
    }

    public ManagementCenterConfig getManagementCenterConfig() {
        return managementCenterConfig;
    }

    public ServicesConfig getServicesConfigConfig() {
        return servicesConfigConfig;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Config");
        sb.append("{groupConfig=").append(groupConfig);
        sb.append(", checkCompatibility=").append(checkCompatibility);
        sb.append(", properties=").append(properties);
        sb.append(", networkConfig=").append(networkConfig);
        sb.append(", mapConfigs=").append(mapConfigs);
        sb.append(", topicConfigs=").append(topicConfigs);
        sb.append(", queueConfigs=").append(queueConfigs);
        sb.append(", multiMapConfigs=").append(multiMapConfigs);
        sb.append(", executorConfigs=").append(executorConfigs);
        sb.append(", semaphoreConfigs=").append(semaphoreConfigs);
        sb.append(", wanReplicationConfigs=").append(wanReplicationConfigs);
        sb.append(", listenerConfigs=").append(listenerConfigs);
        sb.append(", partitionGroupConfig=").append(partitionGroupConfig);
        sb.append(", managementCenterConfig=").append(managementCenterConfig);
        sb.append(", securityConfig=").append(securityConfig);
        sb.append('}');
        return sb.toString();
    }
}
