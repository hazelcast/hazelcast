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

package com.hazelcast.config;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.wan.WanEvent;
import com.hazelcast.wan.WanPublisherState;
import com.hazelcast.wan.WanPublisher;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.isNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Configuration object for the built-in WAN publisher (available in
 * Hazelcast Enterprise). The publisher sends events to another Hazelcast
 * cluster in batches, sending when either when enough events are enqueued
 * or enqueued events have waited for enough time.
 * The publisher can be a different cluster defined by static IP's or
 * discovered using a cloud discovery mechanism.
 *
 * @see DiscoveryConfig
 * @see AwsConfig
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:javadocvariable"})
public class WanBatchPublisherConfig extends AbstractWanPublisherConfig {
    public static final String DEFAULT_CLUSTER_NAME = "dev";
    public static final boolean DEFAULT_SNAPSHOT_ENABLED = false;
    public static final WanPublisherState DEFAULT_INITIAL_PUBLISHER_STATE = WanPublisherState.REPLICATING;
    public static final int DEFAULT_QUEUE_CAPACITY = 10000;
    public static final int DEFAULT_BATCH_SIZE = 500;
    public static final int DEFAULT_BATCH_MAX_DELAY_MILLIS = 1000;
    public static final int DEFAULT_RESPONSE_TIMEOUT_MILLIS = 60000;
    public static final WanQueueFullBehavior DEFAULT_QUEUE_FULL_BEHAVIOUR = WanQueueFullBehavior.DISCARD_AFTER_MUTATION;
    public static final WanAcknowledgeType DEFAULT_ACKNOWLEDGE_TYPE = WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE;
    public static final int DEFAULT_DISCOVERY_PERIOD_SECONDS = 10;
    public static final int DEFAULT_MAX_TARGET_ENDPOINTS = Integer.MAX_VALUE;
    public static final int DEFAULT_MAX_CONCURRENT_INVOCATIONS = -1;
    public static final boolean DEFAULT_USE_ENDPOINT_PRIVATE_ADDRESS = false;
    public static final long DEFAULT_IDLE_MIN_PARK_NS = MILLISECONDS.toNanos(10);
    public static final long DEFAULT_IDLE_MAX_PARK_NS = MILLISECONDS.toNanos(250);
    public static final String DEFAULT_TARGET_ENDPOINTS = "";

    private String clusterName = DEFAULT_CLUSTER_NAME;
    private boolean snapshotEnabled = DEFAULT_SNAPSHOT_ENABLED;
    private WanPublisherState initialPublisherState = DEFAULT_INITIAL_PUBLISHER_STATE;
    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private int batchMaxDelayMillis = DEFAULT_BATCH_MAX_DELAY_MILLIS;
    private int responseTimeoutMillis = DEFAULT_RESPONSE_TIMEOUT_MILLIS;
    private WanQueueFullBehavior queueFullBehavior = DEFAULT_QUEUE_FULL_BEHAVIOUR;
    private WanAcknowledgeType acknowledgeType = DEFAULT_ACKNOWLEDGE_TYPE;

    private int discoveryPeriodSeconds = DEFAULT_DISCOVERY_PERIOD_SECONDS;
    private int maxTargetEndpoints = DEFAULT_MAX_TARGET_ENDPOINTS;
    private int maxConcurrentInvocations = DEFAULT_MAX_CONCURRENT_INVOCATIONS;
    private boolean useEndpointPrivateAddress = DEFAULT_USE_ENDPOINT_PRIVATE_ADDRESS;
    private long idleMinParkNs = DEFAULT_IDLE_MIN_PARK_NS;
    private long idleMaxParkNs = DEFAULT_IDLE_MAX_PARK_NS;

    private String targetEndpoints = DEFAULT_TARGET_ENDPOINTS;

    private AwsConfig awsConfig = new AwsConfig();
    private GcpConfig gcpConfig = new GcpConfig();
    private AzureConfig azureConfig = new AzureConfig();
    private KubernetesConfig kubernetesConfig = new KubernetesConfig();
    private EurekaConfig eurekaConfig = new EurekaConfig();
    private DiscoveryConfig discoveryConfig = new DiscoveryConfig();
    private WanSyncConfig syncConfig = new WanSyncConfig();
    /**
     * WAN endpoint configuration qualifier. When using pre-3.12 network configuration, its value
     * can be {@code null} and is not taken into account. With 3.12+ advanced network config,
     * an {@link EndpointConfig} or {@link ServerSocketEndpointConfig} is looked up with
     * protocol type {@code WAN} and this string as identifier. If such an {@link EndpointConfig}
     * is found, its configuration is used when the WAN publisher opens a connection to the
     * target cluster members.
     */
    private String endpoint;

    public WanBatchPublisherConfig() {
        setClassName("com.hazelcast.enterprise.wan.impl.replication.WanBatchPublisher");
    }

    @Override
    public WanBatchPublisherConfig setClassName(@Nonnull String className) {
        super.setClassName(className);
        return this;
    }

    @Override
    public WanPublisher getImplementation() {
        return null;
    }

    @Override
    public WanBatchPublisherConfig setPublisherId(String publisherId) {
        super.setPublisherId(publisherId);
        return this;
    }

    @Override
    public WanBatchPublisherConfig setProperties(@Nonnull Map<String, Comparable> properties) {
        super.setProperties(properties);
        return this;
    }

    /**
     * No-op method as the implementation cannot be changed for this publisher.
     *
     * @return this config
     */
    @Override
    public WanBatchPublisherConfig setImplementation(@Nonnull WanPublisher implementation) {
        return this;
    }

    /**
     * Returns the cluster name used as a publisher cluster name for authentication
     * on the target endpoint.
     * If there is no separate publisher ID property defined, this cluster name
     * will also be used as a WAN publisher ID. This ID is then used for
     * identifying the publisher in a {@link WanReplicationConfig}.
     *
     * @return the WAN endpoint cluster name
     * @see #getPublisherId()
     */
    public @Nonnull
    String getClusterName() {
        return clusterName;
    }

    /**
     * Sets the cluster name used as an endpoint group password for authentication
     * on the target endpoint.
     * If there is no separate publisher ID property defined, this cluster name
     * will also be used as a WAN publisher ID. This ID is then used for
     * identifying the publisher in a {@link WanReplicationConfig}.
     *
     * @param clusterName the WAN endpoint cluster name
     * @return this config
     * @see #getPublisherId()
     */
    public WanBatchPublisherConfig setClusterName(@Nonnull String clusterName) {
        this.clusterName = checkNotNull(clusterName, "Cluster name must not be null");
        return this;
    }

    /**
     * Returns {@code true} if key-based coalescing is configured for this WAN
     * publisher.
     * When enabled, only the latest {@link WanEvent}
     * of a key is sent to target.
     *
     * @see WanBatchPublisherConfig#isSnapshotEnabled()
     */
    public boolean isSnapshotEnabled() {
        return snapshotEnabled;
    }

    /**
     * Sets if key-based coalescing is configured for this WAN publisher.
     * When enabled, only the latest {@link WanEvent}
     * of a key is sent to target.
     *
     * @return this config
     * @see WanBatchPublisherConfig#isSnapshotEnabled()
     */
    public WanBatchPublisherConfig setSnapshotEnabled(boolean snapshotEnabled) {
        this.snapshotEnabled = snapshotEnabled;
        return this;
    }

    /**
     * Returns the maximum batch size that can be sent to target cluster.
     *
     * @return the maximum size of a WAN event batch
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the maximum batch size that can be sent to target cluster.
     *
     * @param batchSize the maximum size of a WAN event batch
     * @return this config
     */
    public WanBatchPublisherConfig setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Returns the maximum amount of time in milliseconds to wait before sending
     * a batch of events to target cluster, if {@link #getBatchSize()} of events
     * have not arrived within this duration.
     *
     * @return maximum amount of time to wait before sending a batch of events
     */
    public int getBatchMaxDelayMillis() {
        return batchMaxDelayMillis;
    }

    /**
     * Sets the maximum amount of time in milliseconds to wait before sending a
     * batch of events to target cluster, if {@link #getBatchSize()} of events
     * have not arrived within this duration.
     *
     * @param batchMaxDelayMillis maximum amount of time to wait before sending a batch of events
     * @return this config
     */
    public WanBatchPublisherConfig setBatchMaxDelayMillis(int batchMaxDelayMillis) {
        this.batchMaxDelayMillis = batchMaxDelayMillis;
        return this;
    }

    /**
     * Returns the duration in milliseconds for the wait time before retrying to
     * send the events to target cluster again in case the acknowledgement
     * has not arrived.
     *
     * @return timeout for response from target cluster
     */
    public int getResponseTimeoutMillis() {
        return responseTimeoutMillis;
    }

    /**
     * Sets the duration in milliseconds for the waiting time before retrying to
     * send the events to target cluster again in case of acknowledgement
     * is not arrived.
     *
     * @param responseTimeoutMillis timeout for response from target cluster
     * @return this config
     */
    public WanBatchPublisherConfig setResponseTimeoutMillis(int responseTimeoutMillis) {
        this.responseTimeoutMillis = responseTimeoutMillis;
        return this;
    }

    /**
     * Returns the strategy for when the target cluster should acknowledge that
     * a WAN event batch has been processed.
     *
     * @return acknowledge type
     */
    public @Nonnull
    WanAcknowledgeType getAcknowledgeType() {
        return acknowledgeType;
    }

    /**
     * Sets the strategy for when the target cluster should acknowledge that
     * a WAN event batch has been processed.
     *
     * @param acknowledgeType acknowledge type
     * @return this config
     */
    public WanBatchPublisherConfig setAcknowledgeType(@Nonnull WanAcknowledgeType acknowledgeType) {
        this.acknowledgeType = checkNotNull(acknowledgeType, "Acknowledge type name must not be null");
        return this;
    }

    /**
     * Returns the comma separated list of target cluster members,
     * e.g. {@code 127.0.0.1:5701, 127.0.0.1:5702}.
     * If you don't know the addresses of the target cluster members upfront,
     * you may consider using some of the discovery strategies.
     *
     * @return comma separated list
     * @see #getAwsConfig()
     * @see #getGcpConfig()
     * @see #getAzureConfig()
     * @see #getKubernetesConfig()
     * @see #getEurekaConfig()
     * @see #getDiscoveryConfig()
     */
    public String getTargetEndpoints() {
        return targetEndpoints;
    }

    /**
     * Sets the comma separated list of target cluster members,
     * e.g. {@code 127.0.0.1:5701, 127.0.0.1:5702}.
     * If you don't know the addresses of the target cluster members upfront,
     * you may consider using some of the discovery strategies.
     *
     * @param targetEndpoints comma separated list
     * @return this config
     * @see #setAwsConfig(AwsConfig)
     * @see #setGcpConfig(GcpConfig)
     * @see #setAzureConfig(AzureConfig)
     * @see #setKubernetesConfig(KubernetesConfig)
     * @see #setEurekaConfig(EurekaConfig)
     * @see #setDiscoveryConfig(DiscoveryConfig)
     */
    public WanBatchPublisherConfig setTargetEndpoints(String targetEndpoints) {
        this.targetEndpoints = targetEndpoints;
        return this;
    }

    /**
     * Returns the config for the WAN sync mechanism.
     */
    public WanSyncConfig getSyncConfig() {
        return syncConfig;
    }

    /**
     * Sets the config for the WAN sync mechanism.
     *
     * @param syncConfig the WAN sync config
     * @return this config
     */
    public WanBatchPublisherConfig setSyncConfig(WanSyncConfig syncConfig) {
        this.syncConfig = syncConfig;
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
     *
     * @param queueCapacity the queue capacity
     * @return this config
     */
    public WanBatchPublisherConfig setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
        return this;
    }

    /**
     * Returns the configured behaviour of this WAN publisher when the WAN queue
     * is full.
     */
    public @Nonnull
    WanQueueFullBehavior getQueueFullBehavior() {
        return queueFullBehavior;
    }

    /**
     * Sets the configured behaviour of this WAN publisher when the WAN queue is
     * full.
     *
     * @param queueFullBehavior the behaviour of this publisher when the WAN queue is full
     * @return this config
     */
    public WanBatchPublisherConfig setQueueFullBehavior(@Nonnull WanQueueFullBehavior queueFullBehavior) {
        this.queueFullBehavior = checkNotNull(queueFullBehavior, "Queue full behaviour must not be null");
        return this;
    }

    /**
     * Returns the initial WAN publisher state.
     */
    public @Nonnull
    WanPublisherState getInitialPublisherState() {
        return initialPublisherState;
    }

    /**
     * Sets the initial publisher state.
     *
     * @param initialPublisherState the state
     * @return this configuration
     */
    public WanBatchPublisherConfig setInitialPublisherState(@Nonnull WanPublisherState initialPublisherState) {
        this.initialPublisherState = checkNotNull(initialPublisherState, "Initial WAN publisher state must not be null");
        return this;
    }

    /**
     * Returns the period in seconds in which WAN tries to discover new target
     * endpoints and reestablish connections to failed endpoints.
     *
     * @return period for retrying connections to target endpoints
     */
    public int getDiscoveryPeriodSeconds() {
        return discoveryPeriodSeconds;
    }

    /**
     * Sets the period in seconds in which WAN tries to discover new target
     * endpoints and reestablish connections to failed endpoints.
     *
     * @param discoveryPeriodSeconds period for retrying connections to target endpoints
     * @return this config
     */
    public WanBatchPublisherConfig setDiscoveryPeriodSeconds(int discoveryPeriodSeconds) {
        this.discoveryPeriodSeconds = discoveryPeriodSeconds;
        return this;
    }

    /**
     * Returns the maximum number of endpoints that WAN will connect to when
     * using a discovery mechanism to define endpoints.
     * This property has no effect when static endpoint addresses are defined
     * using {@link #setTargetEndpoints(String)}.
     *
     * @return maximum number of endpoints that WAN will connect to
     */
    public int getMaxTargetEndpoints() {
        return maxTargetEndpoints;
    }

    /**
     * Sets the maximum number of endpoints that WAN will connect to when
     * using a discovery mechanism to define endpoints.
     * This property has no effect when static endpoint addresses are defined
     * using {@link #setTargetEndpoints(String)}.
     *
     * @param maxTargetEndpoints maximum number of endpoints that WAN will connect to
     * @return this config
     */
    public WanBatchPublisherConfig setMaxTargetEndpoints(int maxTargetEndpoints) {
        this.maxTargetEndpoints = maxTargetEndpoints;
        return this;
    }

    /**
     * Returns the maximum number of WAN event batches being sent to the target
     * cluster concurrently.
     * <p>
     * Setting this property to anything less than {@code 2} will only allow a
     * single batch of events to be sent to each target endpoint and will
     * maintain causality of events for a single partition.
     * <p>
     * Setting this property to {@code 2} or higher will allow multiple batches
     * of WAN events to be sent to each target endpoint. Since this allows
     * reordering or batches due to network conditions, causality and ordering
     * of events for a single partition is lost and batches for a single
     * partition are now sent randomly to any available target endpoint.
     * This, however, does present faster WAN replication for certain scenarios
     * such as replicating immutable, independent map entries which are only
     * added once and where ordering of when these entries are added is not
     * necessary.
     * Keep in mind that if you set this property to a value which is less than
     * the target endpoint count, you will lose performance as not all target
     * endpoints will be used at any point in time to process WAN event batches.
     * So, for instance, if you have a target cluster with 3 members (target
     * endpoints) and you want to use this property, it makes sense to set it
     * to a value higher than {@code 3}. Otherwise, you can simply disable it
     * by setting it to less than {@code 2} in which case WAN will use the
     * default replication strategy and adapt to the target endpoint count
     * while maintaining causality.
     *
     * @return the maximum number of WAN event batches being sent to the target cluster
     * concurrently
     */
    public int getMaxConcurrentInvocations() {
        return maxConcurrentInvocations;
    }

    /**
     * Sets the maximum number of WAN event batches being sent to the target
     * cluster concurrently.
     * <p>
     * Setting this property to anything less than {@code 2} will only allow a
     * single batch of events to be sent to each target endpoint and will
     * maintain causality of events for a single partition.
     * <p>
     * Setting this property to {@code 2} or higher will allow multiple batches
     * of WAN events to be sent to each target endpoint. Since this allows
     * reordering or batches due to network conditions, causality and ordering
     * of events for a single partition is lost and batches for a single
     * partition are now sent randomly to any available target endpoint.
     * This, however, does present faster WAN replication for certain scenarios
     * such as replicating immutable, independent map entries which are only
     * added once and where ordering of when these entries are added is not
     * necessary.
     * Keep in mind that if you set this property to a value which is less than
     * the target endpoint count, you will lose performance as not all target
     * endpoints will be used at any point in time to process WAN event batches.
     * So, for instance, if you have a target cluster with 3 members (target
     * endpoints) and you want to use this property, it makes sense to set it
     * to a value higher than {@code 3}. Otherwise, you can simply disable it
     * by setting it to less than {@code 2} in which case WAN will use the
     * default replication strategy and adapt to the target endpoint count
     * while maintaining causality.
     *
     * @param maxConcurrentInvocations the maximum number of WAN event batches being sent to the target cluster
     *                                 concurrently
     * @return this config
     */
    public WanBatchPublisherConfig setMaxConcurrentInvocations(int maxConcurrentInvocations) {
        this.maxConcurrentInvocations = maxConcurrentInvocations;
        return this;
    }

    /**
     * Returns whether the WAN connection manager should connect to the
     * endpoint on the private address returned by the discovery SPI.
     * By default this property is {@code false} which means the WAN connection
     * manager will always use the public address.
     *
     * @return {@code true} if the WAN connection manager should connect to the endpoint
     * on the private address returned by the discovery SPI
     * @see DiscoveryNode#getPublicAddress()
     * @see DiscoveryNode#getPrivateAddress()
     */
    public boolean isUseEndpointPrivateAddress() {
        return useEndpointPrivateAddress;
    }

    /**
     * Sets whether the WAN connection manager should connect to the
     * endpoint on the private address returned by the discovery SPI.
     * By default this property is {@code false} which means the WAN connection
     * manager will always use the public address.
     *
     * @return this config
     * @see DiscoveryNode#getPublicAddress()
     * @see DiscoveryNode#getPrivateAddress()
     */
    public WanBatchPublisherConfig setUseEndpointPrivateAddress(boolean useEndpointPrivateAddress) {
        this.useEndpointPrivateAddress = useEndpointPrivateAddress;
        return this;
    }

    /**
     * Returns the minimum duration in nanoseconds that the WAN replication thread
     * will be parked if there are no events to replicate.
     *
     * @return minimum duration in nanoseconds that the WAN replication thread will be
     * parked
     */
    public long getIdleMinParkNs() {
        return idleMinParkNs;
    }

    /**
     * Sets the minimum duration in nanoseconds that the WAN replication thread
     * will be parked if there are no events to replicate.
     *
     * @param idleMinParkNs minimum duration in nanoseconds that the WAN replication thread will be
     *                      parked
     * @return this config
     */
    public WanBatchPublisherConfig setIdleMinParkNs(long idleMinParkNs) {
        this.idleMinParkNs = idleMinParkNs;
        return this;
    }

    /**
     * Returns the maximum duration in nanoseconds that the WAN replication thread
     * will be parked if there are no events to replicate.
     *
     * @return maximum duration in nanoseconds that the WAN replication thread will be
     * parked
     */
    public long getIdleMaxParkNs() {
        return idleMaxParkNs;
    }

    /**
     * Sets the maximum duration in nanoseconds that the WAN replication thread
     * will be parked if there are no events to replicate.
     *
     * @param idleMaxParkNs maximum duration in nanoseconds that the WAN replication thread will be
     *                      parked
     * @return this config
     */
    public WanBatchPublisherConfig setIdleMaxParkNs(long idleMaxParkNs) {
        this.idleMaxParkNs = idleMaxParkNs;
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
    public WanBatchPublisherConfig setAwsConfig(final AwsConfig awsConfig) {
        this.awsConfig = isNotNull(awsConfig, "awsConfig");
        return this;
    }

    /**
     * Returns the {@link GcpConfig} used by the discovery mechanism for this
     * WAN publisher.
     */
    public GcpConfig getGcpConfig() {
        return gcpConfig;
    }

    /**
     * Sets the {@link GcpConfig} used by the discovery mechanism for this
     * WAN publisher.
     *
     * @param gcpConfig the GCP discovery configuration
     * @return this config
     * @throws IllegalArgumentException if gcpConfig is null
     */
    public WanBatchPublisherConfig setGcpConfig(final GcpConfig gcpConfig) {
        this.gcpConfig = isNotNull(gcpConfig, "gcpConfig");
        return this;
    }

    /**
     * Returns the {@link AzureConfig} used by the discovery mechanism for this
     * WAN publisher.
     */
    public AzureConfig getAzureConfig() {
        return azureConfig;
    }

    /**
     * Sets the {@link AzureConfig} used by the discovery mechanism for this
     * WAN publisher.
     *
     * @param azureConfig the Azure discovery configuration
     * @return this config
     * @throws IllegalArgumentException if azureConfig is null
     */
    public WanBatchPublisherConfig setAzureConfig(final AzureConfig azureConfig) {
        this.azureConfig = isNotNull(azureConfig, "azureConfig");
        return this;
    }

    /**
     * Returns the {@link KubernetesConfig} used by the discovery mechanism for this
     * WAN publisher.
     */
    public KubernetesConfig getKubernetesConfig() {
        return kubernetesConfig;
    }

    /**
     * Sets the {@link KubernetesConfig} used by the discovery mechanism for this
     * WAN publisher.
     *
     * @param kubernetesConfig the Kubernetes discovery configuration
     * @return this config
     * @throws IllegalArgumentException if kubernetesConfig is null
     */
    public WanBatchPublisherConfig setKubernetesConfig(final KubernetesConfig kubernetesConfig) {
        this.kubernetesConfig = isNotNull(kubernetesConfig, "kubernetesConfig");
        return this;
    }

    /**
     * Returns the {@link EurekaConfig} used by the discovery mechanism for this
     * WAN publisher.
     */
    public EurekaConfig getEurekaConfig() {
        return eurekaConfig;
    }

    /**
     * Sets the {@link EurekaConfig} used by the discovery mechanism for this
     * WAN publisher.
     *
     * @param eurekaConfig the Eureka discovery configuration
     * @return this config
     * @throws IllegalArgumentException if eurekaConfig is null
     */
    public WanBatchPublisherConfig setEurekaConfig(final EurekaConfig eurekaConfig) {
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
     * @throws IllegalArgumentException if discoveryProvidersConfig is null
     */
    public WanBatchPublisherConfig setDiscoveryConfig(DiscoveryConfig discoveryConfig) {
        this.discoveryConfig = isNotNull(discoveryConfig, "discoveryProvidersConfig");
        return this;
    }

    /**
     * Returns the WAN endpoint configuration qualifier. When using pre-3.12 network
     * configuration, its value can be {@code null} and is not taken into account.
     * With 3.12+ advanced network config, an {@link EndpointConfig} or
     * {@link ServerSocketEndpointConfig} is looked up with protocol type
     * {@code WAN} and this string as identifier. If such an {@link EndpointConfig}
     * is found, its configuration is used when the WAN publisher opens a
     * connection to the target cluster members.
     *
     * @return endpoint qualifier
     * @see NetworkConfig
     * @see AdvancedNetworkConfig
     * @since 3.12
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * Sets the WAN endpoint configuration qualifier. When using pre-3.12 network
     * configuration, its value can be {@code null} and is not taken into account.
     * With 3.12+ advanced network config, an {@link EndpointConfig} or
     * {@link ServerSocketEndpointConfig} is looked up with protocol type
     * {@code WAN} and this string as identifier. If such an {@link EndpointConfig}
     * is found, its configuration is used when the WAN publisher opens a
     * connection to the target cluster members.
     *
     * @param endpoint endpoint qualifier
     * @return this configuration
     * @see NetworkConfig
     * @see AdvancedNetworkConfig
     * @since 3.12
     */
    public WanBatchPublisherConfig setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    @Override
    public String toString() {
        return "WanBatchReplicationPublisherConfig{"
                + "clusterName='" + clusterName + '\''
                + ", publisherId='" + publisherId + '\''
                + ", queueCapacity=" + queueCapacity
                + ", queueFullBehavior=" + queueFullBehavior
                + ", initialPublisherState=" + initialPublisherState
                + ", wanSyncConfig=" + syncConfig
                + ", properties=" + properties
                + ", className='" + className + '\''
                + ", implementation=" + implementation
                + ", awsConfig=" + awsConfig
                + ", gcpConfig=" + gcpConfig
                + ", azureConfig=" + azureConfig
                + ", kubernetesConfig=" + kubernetesConfig
                + ", eurekaConfig=" + eurekaConfig
                + ", discoveryConfig=" + discoveryConfig
                + ", endpoint=" + endpoint
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.WAN_BATCH_PUBLISHER_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeString(clusterName);
        out.writeBoolean(snapshotEnabled);
        out.writeByte(initialPublisherState.getId());
        out.writeInt(queueCapacity);
        out.writeInt(batchSize);
        out.writeInt(batchMaxDelayMillis);
        out.writeInt(responseTimeoutMillis);
        out.writeInt(queueFullBehavior.getId());
        out.writeInt(acknowledgeType.getId());
        out.writeInt(discoveryPeriodSeconds);
        out.writeInt(maxTargetEndpoints);
        out.writeInt(maxConcurrentInvocations);
        out.writeBoolean(useEndpointPrivateAddress);
        out.writeLong(idleMinParkNs);
        out.writeLong(idleMaxParkNs);
        out.writeString(targetEndpoints);
        out.writeObject(awsConfig);
        out.writeObject(gcpConfig);
        out.writeObject(azureConfig);
        out.writeObject(kubernetesConfig);
        out.writeObject(eurekaConfig);
        out.writeObject(discoveryConfig);
        out.writeObject(syncConfig);
        out.writeString(endpoint);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        clusterName = in.readString();
        snapshotEnabled = in.readBoolean();
        initialPublisherState = WanPublisherState.getByType(in.readByte());
        queueCapacity = in.readInt();
        batchSize = in.readInt();
        batchMaxDelayMillis = in.readInt();
        responseTimeoutMillis = in.readInt();
        queueFullBehavior = WanQueueFullBehavior.getByType(in.readInt());
        acknowledgeType = WanAcknowledgeType.getById(in.readInt());
        discoveryPeriodSeconds = in.readInt();
        maxTargetEndpoints = in.readInt();
        maxConcurrentInvocations = in.readInt();
        useEndpointPrivateAddress = in.readBoolean();
        idleMinParkNs = in.readLong();
        idleMaxParkNs = in.readLong();
        targetEndpoints = in.readString();
        awsConfig = in.readObject();
        gcpConfig = in.readObject();
        azureConfig = in.readObject();
        kubernetesConfig = in.readObject();
        eurekaConfig = in.readObject();
        discoveryConfig = in.readObject();
        syncConfig = in.readObject();
        endpoint = in.readString();
    }


}
