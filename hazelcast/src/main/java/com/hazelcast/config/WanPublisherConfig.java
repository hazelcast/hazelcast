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

package com.hazelcast.config;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Configuration object for a WAN publisher. A single publisher defines how
 * WAN events are sent to a specific endpoint.
 * The endpoint can be a different cluster defined by static IP's or discovered
 * using a cloud discovery mechanism. When using a custom WAN publisher
 * implementation, the target may also be some other external system which is
 * not a Hazelcast cluster.
 *
 * @see DiscoveryConfig
 * @see AwsConfig
 */
@SuppressWarnings("checkstyle:methodcount")
public class WanPublisherConfig implements IdentifiedDataSerializable, Versioned {

    private static final int DEFAULT_QUEUE_CAPACITY = 10000;
    private static final WANQueueFullBehavior DEFAULT_QUEUE_FULL_BEHAVIOR = WANQueueFullBehavior.DISCARD_AFTER_MUTATION;

    private String groupName = "dev";
    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
    private WANQueueFullBehavior queueFullBehavior = DEFAULT_QUEUE_FULL_BEHAVIOR;
    private WanPublisherState initialPublisherState = WanPublisherState.REPLICATING;
    private Map<String, Comparable> properties = new HashMap<String, Comparable>();
    private String className;
    private Object implementation;
    private AwsConfig awsConfig = new AwsConfig();
    private GcpConfig gcpConfig = new GcpConfig();
    private AzureConfig azureConfig = new AzureConfig();
    private KubernetesConfig kubernetesConfig = new KubernetesConfig();
    private EurekaConfig eurekaConfig = new EurekaConfig();
    private DiscoveryConfig discoveryConfig = new DiscoveryConfig();
    private WanSyncConfig wanSyncConfig = new WanSyncConfig();

    /**
     * Returns the config for the WAN sync mechanism.
     */
    public WanSyncConfig getWanSyncConfig() {
        return wanSyncConfig;
    }

    /**
     * Sets the config for the WAN sync mechanism.
     *
     * @param wanSyncConfig the WAN sync config
     * @return this config
     */
    public WanPublisherConfig setWanSyncConfig(WanSyncConfig wanSyncConfig) {
        this.wanSyncConfig = wanSyncConfig;
        return this;
    }

    /**
     * Returns the WAN publisher name used for identifying the publisher in
     * a {@link WanReplicationConfig}.
     * <p>
     * This name will also be used as an endpoint group name for authentication
     * on the target endpoint.
     * <p>
     * The group name can also be defined in the publisher properties which
     * takes precedence over this value. In such cases, this value will only
     * be used as part of a WAN publisher identifier and the value defined in
     * the properties will be used for authentication.
     * This value must be unique for a single WAN replication scheme.
     *
     * @return the WAN publisher name and endpoint group name
     */
    public String getGroupName() {
        return groupName;
    }

    /**
     * Sets the WAN publisher name used for identifying the publisher in
     * a {@link WanReplicationConfig}.
     * <p>
     * This name will also be used as an endpoint group name for authentication
     * on the target endpoint.
     * <p>
     * The group name can also be defined in the publisher properties which
     * takes precedence over this value. In such cases, this value will only
     * be used as part of a WAN publisher identifier and the value defined in
     * the properties will be used for authentication.
     * This value must be unique for a single WAN replication scheme.
     *
     * @param groupName the WAN publisher name and publisher group name
     * @return this config
     */
    public WanPublisherConfig setGroupName(String groupName) {
        this.groupName = groupName;
        return this;
    }

    /**
     * Returns the capacity of the primary and backup queue for WAN replication events.
     * <p>
     * One hazelcast instance can have up to {@code 2*queueCapacity} events since
     * we keep up to {@code queueCapacity} primary events (events with keys for
     * which the instance is the owner) and {@code queueCapacity} backup events
     * (events with keys for which the instance is the backup).
     * Events for IMap and ICache count against this limit collectively.
     * <p>
     * When the queue capacity is reached, backup events are dropped while normal
     * replication events behave as determined by the {@link #getQueueFullBehavior()}.
     * The default queue size for replication queues is {@value #DEFAULT_QUEUE_CAPACITY}.
     *
     * @return the queue capacity
     */
    public int getQueueCapacity() {
        return queueCapacity;
    }

    /**
     * Sets the capacity of the primary and backup queue for WAN replication events.
     * <p>
     * One hazelcast instance can have up to {@code 2*queueCapacity} events since
     * we keep up to {@code queueCapacity} primary events (events with keys for
     * which the instance is the owner) and {@code queueCapacity} backup events
     * (events with keys for which the instance is the backup).
     * Events for IMap and ICache count against this limit collectively.
     * <p>
     * When the queue capacity is reached, backup events are dropped while normal
     * replication events behave as determined by the {@link #getQueueFullBehavior()}.
     * The default queue size for replication queues is {@value #DEFAULT_QUEUE_CAPACITY}.
     *
     * @param queueCapacity the queue capacity
     * @return this config
     */
    public WanPublisherConfig setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
        return this;
    }

    /**
     * Returns the configured behaviour of this WAN publisher when the WAN queue
     * is full.
     */
    public WANQueueFullBehavior getQueueFullBehavior() {
        return queueFullBehavior;
    }

    /**
     * Sets the configured behaviour of this WAN publisher when the WAN queue is
     * full.
     *
     * @param queueFullBehavior the behaviour of this publisher when the WAN queue is full
     * @return this config
     */
    public WanPublisherConfig setQueueFullBehavior(WANQueueFullBehavior queueFullBehavior) {
        this.queueFullBehavior = queueFullBehavior;
        return this;
    }

    /**
     * Returns the initial WAN publisher state.
     */
    public WanPublisherState getInitialPublisherState() {
        return initialPublisherState;
    }

    /**
     * Sets the initial publisher state.
     *
     * @param initialPublisherState the state
     * @return this configuration
     */
    public WanPublisherConfig setInitialPublisherState(WanPublisherState initialPublisherState) {
        checkNotNull(initialPublisherState, "Initial WAN publisher state must not be null");
        this.initialPublisherState = initialPublisherState;
        return this;
    }

    /**
     * Returns the WAN publisher properties.
     */
    public Map<String, Comparable> getProperties() {
        return properties;
    }

    /**
     * Sets the WAN publisher properties.
     *
     * @param properties WAN publisher properties
     * @return this config
     */
    public WanPublisherConfig setProperties(Map<String, Comparable> properties) {
        this.properties = properties;
        return this;
    }

    /**
     * Returns the name of the class implementing the WanReplicationEndpoint.
     * NOTE: OS and EE have different interfaces that this class should implement.
     * For OS see {@link com.hazelcast.wan.WanReplicationEndpoint}.
     */
    public String getClassName() {
        return className;
    }

    /**
     * Sets the name of the class implementing the WanReplicationEndpoint.
     * NOTE: OS and EE have different interfaces that this class should implement.
     * For OS see {@link com.hazelcast.wan.WanReplicationEndpoint}.
     *
     * @param className the name of the class implementation for the WAN replication
     * @return this config
     */
    public WanPublisherConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    /**
     * Returns the implementation of the WanReplicationEndpoint.
     * NOTE: OS and EE have different interfaces that this object should implement.
     * For OS see {@link com.hazelcast.wan.WanReplicationEndpoint}.
     */
    public Object getImplementation() {
        return implementation;
    }

    /**
     * Sets the implementation of the WanReplicationEndpoint.
     * NOTE: OS and EE have different interfaces that this object should implement.
     * For OS see {@link com.hazelcast.wan.WanReplicationEndpoint}.
     *
     * @param implementation the implementation for the WAN replication
     * @return this config
     */
    public WanPublisherConfig setImplementation(Object implementation) {
        this.implementation = implementation;
        return this;
    }

    /**
     * Returns the {@link AwsConfig} used by the discovery mechanism for this
     * WAN publisher.
     */
    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    /**
     * Sets the {@link AwsConfig} used by the discovery mechanism for this
     * WAN publisher.
     *
     * @param awsConfig the AWS discovery configuration
     * @return this config
     * @throws IllegalArgumentException if awsConfig is null
     */
    public WanPublisherConfig setAwsConfig(final AwsConfig awsConfig) {
        this.awsConfig = isNotNull(awsConfig, "awsConfig");
        return this;
    }

    public GcpConfig getGcpConfig() {
        return gcpConfig;
    }

    public WanPublisherConfig setGcpConfig(final GcpConfig gcpConfig) {
        this.gcpConfig = isNotNull(gcpConfig, "gcpConfig");
        return this;
    }

    public AzureConfig getAzureConfig() {
        return azureConfig;
    }

    public WanPublisherConfig setAzureConfig(final AzureConfig azureConfig) {
        this.azureConfig = isNotNull(azureConfig, "azureConfig");
        return this;
    }

    public KubernetesConfig getKubernetesConfig() {
        return kubernetesConfig;
    }

    public WanPublisherConfig setKubernetesConfig(final KubernetesConfig kubernetesConfig) {
        this.kubernetesConfig = isNotNull(kubernetesConfig, "kubernetesConfig");
        return this;
    }

    public EurekaConfig getEurekaConfig() {
        return eurekaConfig;
    }

    public WanPublisherConfig setEurekaConfig(final EurekaConfig eurekaConfig) {
        this.eurekaConfig = isNotNull(eurekaConfig, "eurekaConfig");
        return this;
    }

    /**
     * Returns the currently defined {@link DiscoveryConfig} used by the
     * discovery mechanism for this WAN publisher.
     *
     * @return current DiscoveryProvidersConfig instance
     */
    public DiscoveryConfig getDiscoveryConfig() {
        return discoveryConfig;
    }

    /**
     * Sets the {@link DiscoveryConfig} used by the discovery mechanism for
     * this WAN publisher.
     *
     * @param discoveryConfig configuration to set
     * @return this config
     * @throws java.lang.IllegalArgumentException if discoveryProvidersConfig is null
     */
    public WanPublisherConfig setDiscoveryConfig(DiscoveryConfig discoveryConfig) {
        this.discoveryConfig = isNotNull(discoveryConfig, "discoveryProvidersConfig");
        return this;
    }

    @Override
    public String toString() {
        return "WanPublisherConfig{"
                + "groupName='" + groupName + '\''
                + ", queueCapacity=" + queueCapacity
                + ", queueFullBehavior=" + queueFullBehavior
                + ", initialPublisherState=" + initialPublisherState
                + ", wanSyncConfig=" + wanSyncConfig
                + ", properties=" + properties
                + ", className='" + className + '\''
                + ", implementation=" + implementation
                + ", awsConfig=" + awsConfig
                + ", gcpConfig=" + gcpConfig
                + ", azureConfig=" + azureConfig
                + ", kubernetesConfig=" + kubernetesConfig
                + ", eurekaConfig=" + eurekaConfig
                + ", discoveryConfig=" + discoveryConfig
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.WAN_PUBLISHER_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(groupName);
        out.writeInt(queueCapacity);
        out.writeInt(queueFullBehavior.getId());
        int size = properties.size();
        out.writeInt(size);
        for (Map.Entry<String, Comparable> entry : properties.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
        out.writeUTF(className);
        out.writeObject(implementation);

        // RU_COMPAT_3_10
        if (out.getVersion().isGreaterOrEqual(Versions.V3_11)) {
            out.writeByte(initialPublisherState.getId());
            out.writeObject(wanSyncConfig);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupName = in.readUTF();
        queueCapacity = in.readInt();
        queueFullBehavior = WANQueueFullBehavior.getByType(in.readInt());
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            properties.put(in.readUTF(), (Comparable) in.readObject());
        }
        className = in.readUTF();
        implementation = in.readObject();

        // RU_COMPAT_3_10
        if (in.getVersion().isGreaterOrEqual(Versions.V3_11)) {
            initialPublisherState = WanPublisherState.getByType(in.readByte());
            wanSyncConfig = in.readObject();
        }
    }
}
