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

package com.hazelcast.internal.config;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.merge.policy.CacheMergePolicyProvider;
import com.hazelcast.config.AbstractBasicConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CollectionConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.spi.merge.SplitBrainMergeTypeProvider;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.util.MutableInteger;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;

import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MapConfig.DEFAULT_EVICTION_PERCENTAGE;
import static com.hazelcast.config.MapConfig.DEFAULT_MIN_EVICTION_CHECK_MILLIS;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_NODE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_PARTITION;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.INVALIDATE;
import static com.hazelcast.instance.BuildInfoProvider.getBuildInfo;
import static com.hazelcast.instance.ProtocolType.MEMBER;
import static com.hazelcast.instance.ProtocolType.REST;
import static com.hazelcast.instance.ProtocolType.WAN;
import static com.hazelcast.internal.config.MergePolicyValidator.checkCacheMergePolicy;
import static com.hazelcast.internal.config.MergePolicyValidator.checkMapMergePolicy;
import static com.hazelcast.internal.config.MergePolicyValidator.checkMergePolicy;
import static com.hazelcast.internal.config.MergePolicyValidator.checkReplicatedMapMergePolicy;
import static com.hazelcast.spi.properties.GroupProperty.HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.spi.properties.GroupProperty.HTTP_HEALTHCHECK_ENABLED;
import static com.hazelcast.spi.properties.GroupProperty.MEMCACHE_ENABLED;
import static com.hazelcast.spi.properties.GroupProperty.REST_ENABLED;
import static com.hazelcast.util.Preconditions.checkTrue;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;
import static java.lang.String.format;

/**
 * Validates a Hazelcast configuration in a specific
 * context like OS vs. EE or client vs. member nodes.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public final class ConfigValidator {

    private static final EnumSet<MaxSizePolicy> SUPPORTED_ON_HEAP_NEAR_CACHE_MAXSIZE_POLICIES
            = EnumSet.of(MaxSizePolicy.ENTRY_COUNT);

    private static final EnumSet<MaxSizeConfig.MaxSizePolicy> SUPPORTED_NATIVE_MAX_SIZE_POLICIES
            = EnumSet.of(PER_NODE, PER_PARTITION, USED_NATIVE_MEMORY_PERCENTAGE,
            FREE_NATIVE_MEMORY_PERCENTAGE, USED_NATIVE_MEMORY_SIZE, FREE_NATIVE_MEMORY_SIZE);

    private static final EnumSet<EvictionPolicy> SUPPORTED_EVICTION_POLICIES = EnumSet.of(LRU, LFU);

    private static final ILogger LOGGER = Logger.getLogger(ConfigValidator.class);

    private ConfigValidator() {
    }

    /**
     * Validates the given {@link MapConfig}.
     *
     * @param mapConfig           the {@link MapConfig}
     * @param mergePolicyProvider the {@link MergePolicyProvider} to resolve merge policy classes
     */
    public static void checkMapConfig(MapConfig mapConfig, NativeMemoryConfig nativeMemoryConfig,
                                      MergePolicyProvider mergePolicyProvider, HazelcastProperties properties) {
        checkNotNativeWhenOpenSource(mapConfig.getInMemoryFormat());

        boolean enterprise = getBuildInfo().isEnterprise();
        if (enterprise) {
            checkNativeConfig(mapConfig, nativeMemoryConfig);
            checkHotRestartSpecificConfig(mapConfig, properties);
        }
        checkMapMergePolicy(mapConfig, mergePolicyProvider);
        logIgnoredConfig(mapConfig);
    }

    @SuppressWarnings("deprecation")
    private static void logIgnoredConfig(MapConfig mapConfig) {
        if (mapConfig.getMinEvictionCheckMillis() != DEFAULT_MIN_EVICTION_CHECK_MILLIS
                || mapConfig.getEvictionPercentage() != DEFAULT_EVICTION_PERCENTAGE) {
            LOGGER.warning("As of Hazelcast version 3.7 `minEvictionCheckMillis` and `evictionPercentage`"
                    + " are deprecated due to a change of the eviction mechanism."
                    + " The new eviction mechanism uses a probabilistic algorithm based on sampling."
                    + " Please see documentation for further details.");
        }
    }

    /**
     * Checks preconditions to create a map proxy.
     *
     * @param mapConfig          the mapConfig
     * @param nativeMemoryConfig the nativeMemoryConfig
     */
    private static void checkNativeConfig(MapConfig mapConfig, NativeMemoryConfig nativeMemoryConfig) {
        if (NATIVE != mapConfig.getInMemoryFormat()) {
            return;
        }
        checkTrue(nativeMemoryConfig.isEnabled(),
                format("Enable native memory config to use NATIVE in-memory-format for the map [%s]", mapConfig.getName()));
        checkNativeMaxSizePolicy(mapConfig);
    }

    private static void checkNativeMaxSizePolicy(MapConfig mapConfig) {
        MaxSizeConfig maxSizeConfig = mapConfig.getMaxSizeConfig();
        MaxSizeConfig.MaxSizePolicy maxSizePolicy = maxSizeConfig.getMaxSizePolicy();
        if (!SUPPORTED_NATIVE_MAX_SIZE_POLICIES.contains(maxSizePolicy)) {
            throw new IllegalArgumentException("Map maximum size policy " + maxSizePolicy
                    + " cannot be used with NATIVE in memory format."
                    + " Supported maximum size policies are: " + SUPPORTED_NATIVE_MAX_SIZE_POLICIES);
        }
    }

    /**
     * When Hot Restart is enabled, we want at least {@code
     * hazelcast.hotrestart.free.native.memory.percentage}
     * percent free HD memory space.
     * <p>
     * If configured max-size-policy is {@link
     * MaxSizeConfig.MaxSizePolicy#FREE_NATIVE_MEMORY_PERCENTAGE},
     * this method asserts that max-size is not below {@code
     * hazelcast.hotrestart.free.native.memory.percentage}.
     */
    private static void checkHotRestartSpecificConfig(MapConfig mapConfig, HazelcastProperties properties) {
        HotRestartConfig hotRestartConfig = mapConfig.getHotRestartConfig();
        if (hotRestartConfig == null || !hotRestartConfig.isEnabled()) {
            return;
        }
        int hotRestartMinFreeNativeMemoryPercentage = properties.getInteger(HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE);
        MaxSizeConfig maxSizeConfig = mapConfig.getMaxSizeConfig();
        MaxSizeConfig.MaxSizePolicy maxSizePolicy = maxSizeConfig.getMaxSizePolicy();
        int localSizeConfig = maxSizeConfig.getSize();
        if (FREE_NATIVE_MEMORY_PERCENTAGE == maxSizePolicy && localSizeConfig < hotRestartMinFreeNativeMemoryPercentage) {
            throw new IllegalArgumentException(format(
                    "There is a global limit on the minimum free native memory, configurable by the system property %s,"
                            + " whose value is currently %d percent. The map %s has Hot Restart enabled,"
                            + " but is configured with %d percent, which is lower than the allowed minimum.",
                    HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE.getName(), hotRestartMinFreeNativeMemoryPercentage,
                    mapConfig.getName(), localSizeConfig)
            );
        }
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity",
            "checkstyle:booleanexpressioncomplexity"})
    public static void checkAdvancedNetworkConfig(Config config) {
        if (!config.getAdvancedNetworkConfig().isEnabled()) {
            return;
        }

        EnumMap<ProtocolType, MutableInteger> serverSocketsPerProtocolType
                = new EnumMap<ProtocolType, MutableInteger>(ProtocolType.class);
        for (ProtocolType protocolType : ProtocolType.values()) {
            serverSocketsPerProtocolType.put(protocolType, new MutableInteger());
        }

        Map<EndpointQualifier, EndpointConfig> endpointConfigs = config.getAdvancedNetworkConfig().getEndpointConfigs();

        for (EndpointConfig endpointConfig : endpointConfigs.values()) {
            if (endpointConfig instanceof ServerSocketEndpointConfig) {
                serverSocketsPerProtocolType.get(endpointConfig.getProtocolType()).getAndInc();
            }
        }

        for (ProtocolType protocolType : ProtocolType.values()) {
            int serverSocketCount = serverSocketsPerProtocolType.get(protocolType).value;
            if (serverSocketCount > protocolType.getServerSocketCardinality()) {
                throw new InvalidConfigurationException(format("Protocol type %s allows definition "
                                + "of up to %d server sockets but %d were configured", protocolType,
                        protocolType.getServerSocketCardinality(), serverSocketCount));
            }
        }

        // ensure there is 1 MEMBER type server socket
        if (serverSocketsPerProtocolType.get(MEMBER).value != 1) {
            throw new InvalidConfigurationException("A member-server-socket-endpoint"
                    + " configuration is required for the cluster to form.");
        }

        HazelcastProperties props = new HazelcastProperties(config);
        if (props.getBoolean(REST_ENABLED) || props.getBoolean(HTTP_HEALTHCHECK_ENABLED)) {
            if (serverSocketsPerProtocolType.get(REST).value != 1) {
                throw new InvalidConfigurationException("`hazelcast.rest.enabled` and/or "
                        + "`hazelcast.http.healthcheck.enabled` properties are enabled, without a rest-server-socket-endpoint");
            }
        }

        if (props.getBoolean(MEMCACHE_ENABLED)) {
            if (serverSocketsPerProtocolType.get(REST).value != 1) {
                throw new InvalidConfigurationException("`hazelcast.memcache.enabled` property is enabled, without "
                        + "a memcache-server-socket-endpoint");
            }
        }

        // endpoint qualifiers referenced by WAN publishers must exist
        for (WanReplicationConfig wanReplicationConfig : config.getWanReplicationConfigs().values()) {
            for (WanPublisherConfig wanPublisherConfig : wanReplicationConfig.getWanPublisherConfigs()) {
                if (wanPublisherConfig.getEndpoint() != null) {
                    EndpointQualifier qualifier = EndpointQualifier.resolve(WAN, wanPublisherConfig.getEndpoint());
                    if (endpointConfigs.get(qualifier) == null) {
                        throw new InvalidConfigurationException(
                                format("WAN publisher config for group name '%s' requires an wan-endpoint "
                                                + "config with identifier '%s' but none was found",
                                        wanPublisherConfig.getGroupName(), wanPublisherConfig.getEndpoint()));
                    }
                }
            }
        }
    }

    /**
     * Checks preconditions to create a map proxy with Near Cache.
     *
     * @param mapName            name of the map that Near Cache will be created for
     * @param nearCacheConfig    the {@link NearCacheConfig} to be checked
     * @param nativeMemoryConfig the {@link NativeMemoryConfig} of the Hazelcast instance
     * @param isClient           {@code true} if the config is for a Hazelcast client, {@code false} otherwise
     */
    public static void checkNearCacheConfig(String mapName, NearCacheConfig nearCacheConfig,
                                            NativeMemoryConfig nativeMemoryConfig, boolean isClient) {
        checkNotNativeWhenOpenSource(nearCacheConfig.getInMemoryFormat());
        checkLocalUpdatePolicy(mapName, nearCacheConfig.getLocalUpdatePolicy());
        checkEvictionConfig(nearCacheConfig.getEvictionConfig(), true);
        checkOnHeapNearCacheMaxSizePolicy(nearCacheConfig);
        checkNearCacheNativeMemoryConfig(nearCacheConfig.getInMemoryFormat(),
                nativeMemoryConfig, getBuildInfo().isEnterprise());

        if (isClient && nearCacheConfig.isCacheLocalEntries()) {
            throw new IllegalArgumentException("The Near Cache option `cache-local-entries` is not supported in "
                    + "client configurations.");
        }
        checkPreloaderConfig(nearCacheConfig, isClient);
    }

    /**
     * Checks IMap's supported Near Cache local update policy configuration.
     *
     * @param mapName           name of the map that Near Cache will be created for
     * @param localUpdatePolicy local update policy
     */
    private static void checkLocalUpdatePolicy(String mapName, LocalUpdatePolicy localUpdatePolicy) {
        if (localUpdatePolicy != INVALIDATE) {
            throw new IllegalArgumentException(format("Wrong `local-update-policy` option is selected for `%s` map Near Cache."
                    + " Only `%s` option is supported but found `%s`", mapName, INVALIDATE, localUpdatePolicy));
        }
    }

    /**
     * Checks if a {@link EvictionConfig} is valid in its context.
     *
     * @param evictionConfig the {@link EvictionConfig}
     * @param isNearCache    {@code true} if the config is for a Near Cache, {@code false} otherwise
     */
    @SuppressWarnings("ConstantConditions")
    public static void checkEvictionConfig(EvictionConfig evictionConfig, boolean isNearCache) {
        if (evictionConfig == null) {
            throw new IllegalArgumentException("Eviction config cannot be null!");
        }
        EvictionPolicy evictionPolicy = evictionConfig.getEvictionPolicy();
        String comparatorClassName = evictionConfig.getComparatorClassName();
        EvictionPolicyComparator comparator = evictionConfig.getComparator();

        checkEvictionConfig(evictionPolicy, comparatorClassName, comparator, isNearCache);
    }

    private static void checkOnHeapNearCacheMaxSizePolicy(NearCacheConfig nearCacheConfig) {
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (inMemoryFormat == NATIVE) {
            return;
        }

        MaxSizePolicy maxSizePolicy = nearCacheConfig.getEvictionConfig().getMaximumSizePolicy();
        if (!SUPPORTED_ON_HEAP_NEAR_CACHE_MAXSIZE_POLICIES.contains(maxSizePolicy)) {
            throw new IllegalArgumentException(format("Near Cache maximum size policy %s cannot be used with %s storage."
                            + " Supported maximum size policies are: %s",
                    maxSizePolicy, inMemoryFormat, SUPPORTED_ON_HEAP_NEAR_CACHE_MAXSIZE_POLICIES));
        }
    }

    /**
     * Checks precondition to use {@link InMemoryFormat#NATIVE}.
     *
     * @param inMemoryFormat     the {@link InMemoryFormat} of the Near Cache
     * @param nativeMemoryConfig the {@link NativeMemoryConfig} of the Hazelcast instance
     * @param isEnterprise       {@code true} if the Hazelcast instance is EE, {@code false} otherwise
     */
    static void checkNearCacheNativeMemoryConfig(InMemoryFormat inMemoryFormat, NativeMemoryConfig nativeMemoryConfig,
                                                 boolean isEnterprise) {
        if (!isEnterprise) {
            return;
        }
        if (inMemoryFormat != NATIVE) {
            return;
        }
        if (nativeMemoryConfig != null && nativeMemoryConfig.isEnabled()) {
            return;
        }
        throw new IllegalArgumentException("Enable native memory config to use NATIVE in-memory-format for Near Cache");
    }

    /**
     * Checks if parameters for an {@link EvictionConfig} are valid in their context.
     *
     * @param evictionPolicy      the {@link EvictionPolicy} for the {@link EvictionConfig}
     * @param comparatorClassName the comparator class name for the {@link EvictionConfig}
     * @param comparator          the comparator implementation for the {@link EvictionConfig}
     * @param isNearCache         {@code true} if the config is for a Near Cache, {@code false} otherwise
     */
    public static void checkEvictionConfig(EvictionPolicy evictionPolicy, String comparatorClassName, Object comparator,
                                           boolean isNearCache) {
        if (comparatorClassName != null && comparator != null) {
            throw new IllegalArgumentException("Only one of the `comparator class name` and `comparator`"
                    + " can be configured in the eviction configuration!");
        }
        if (!isNearCache && !SUPPORTED_EVICTION_POLICIES.contains(evictionPolicy)) {
            if (isNullOrEmpty(comparatorClassName) && comparator == null) {

                String msg = format("Eviction policy `%s` is not supported. Either you can provide a custom one or "
                        + "can use one of the supported: %s.", evictionPolicy, SUPPORTED_EVICTION_POLICIES);

                throw new IllegalArgumentException(msg);
            }
        } else {
            if (evictionPolicy != EvictionConfig.DEFAULT_EVICTION_POLICY) {
                if (!isNullOrEmpty(comparatorClassName)) {
                    throw new IllegalArgumentException(
                            "Only one of the `eviction policy` and `comparator class name` can be configured!");
                }
                if (comparator != null) {
                    throw new IllegalArgumentException("Only one of the `eviction policy` and `comparator` can be configured!");
                }
            }
        }
    }

    /**
     * Validates the given {@link CacheSimpleConfig}.
     *
     * @param cacheSimpleConfig   the {@link CacheSimpleConfig} to check
     * @param mergePolicyProvider the {@link CacheMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkCacheConfig(CacheSimpleConfig cacheSimpleConfig, CacheMergePolicyProvider mergePolicyProvider) {
        checkCacheConfig(cacheSimpleConfig.getInMemoryFormat(), cacheSimpleConfig.getEvictionConfig(),
                cacheSimpleConfig.getMergePolicy(), cacheSimpleConfig, mergePolicyProvider);
    }

    /**
     * Validates the given {@link CacheConfig}.
     *
     * @param cacheConfig         the {@link CacheConfig} to check
     * @param mergePolicyProvider the {@link CacheMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkCacheConfig(CacheConfig cacheConfig, CacheMergePolicyProvider mergePolicyProvider) {
        checkCacheConfig(cacheConfig.getInMemoryFormat(), cacheConfig.getEvictionConfig(), cacheConfig.getMergePolicy(),
                cacheConfig, mergePolicyProvider);
    }

    /**
     * Validates the given parameters in the context of an {@link ICache} config.
     *
     * @param inMemoryFormat       the {@link InMemoryFormat} of the cache
     * @param evictionConfig       the {@link EvictionConfig} of the cache
     * @param mergePolicyClassname the configured merge policy of the cache
     * @param mergeTypeProvider    the {@link SplitBrainMergeTypeProvider} of the cache
     * @param mergePolicyProvider  the {@link CacheMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkCacheConfig(InMemoryFormat inMemoryFormat, EvictionConfig evictionConfig, String mergePolicyClassname,
                                        SplitBrainMergeTypeProvider mergeTypeProvider,
                                        CacheMergePolicyProvider mergePolicyProvider) {
        checkNotNativeWhenOpenSource(inMemoryFormat);
        checkEvictionConfig(inMemoryFormat, evictionConfig);
        checkCacheMergePolicy(mergePolicyClassname, mergeTypeProvider, mergePolicyProvider);
    }

    /**
     * Checks the merge policy configuration in the context of an {@link ICache}.
     *
     * @param inMemoryFormat the {@link InMemoryFormat} of the cache
     * @param evictionConfig the {@link EvictionConfig} of the cache
     */
    static void checkEvictionConfig(InMemoryFormat inMemoryFormat, EvictionConfig evictionConfig) {
        if (inMemoryFormat == NATIVE) {
            MaxSizePolicy maxSizePolicy = evictionConfig.getMaximumSizePolicy();
            if (maxSizePolicy == MaxSizePolicy.ENTRY_COUNT) {
                throw new IllegalArgumentException("Invalid max-size policy "
                        + '(' + maxSizePolicy + ") for NATIVE in-memory format! Only "
                        + MaxSizePolicy.USED_NATIVE_MEMORY_SIZE + ", "
                        + MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE + ", "
                        + MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE + ", "
                        + MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE
                        + " are supported.");
            }
        }
    }

    /**
     * Validates the given {@link ReplicatedMapConfig}.
     *
     * @param replicatedMapConfig the {@link ReplicatedMapConfig} to check
     * @param mergePolicyProvider the {@link com.hazelcast.replicatedmap.merge.MergePolicyProvider}
     *                            to resolve merge policy classes
     */
    public static void checkReplicatedMapConfig(ReplicatedMapConfig replicatedMapConfig,
                                                com.hazelcast.replicatedmap.merge.MergePolicyProvider mergePolicyProvider) {
        checkReplicatedMapMergePolicy(replicatedMapConfig, mergePolicyProvider);
    }

    /**
     * Validates the given {@link MultiMapConfig}.
     *
     * @param multiMapConfig      the {@link MultiMapConfig} to check
     * @param mergePolicyProvider the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkMultiMapConfig(MultiMapConfig multiMapConfig,
                                           SplitBrainMergePolicyProvider mergePolicyProvider) {
        checkMergePolicy(multiMapConfig, mergePolicyProvider, multiMapConfig.getMergePolicyConfig().getPolicy());
    }

    /**
     * Validates the given {@link QueueConfig}.
     *
     * @param queueConfig         the {@link QueueConfig} to check
     * @param mergePolicyProvider the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkQueueConfig(QueueConfig queueConfig, SplitBrainMergePolicyProvider mergePolicyProvider) {
        checkMergePolicy(queueConfig, mergePolicyProvider, queueConfig.getMergePolicyConfig().getPolicy());
    }

    /**
     * Validates the given {@link CollectionConfig}.
     *
     * @param collectionConfig    the {@link CollectionConfig} to check
     * @param mergePolicyProvider the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkCollectionConfig(CollectionConfig collectionConfig,
                                             SplitBrainMergePolicyProvider mergePolicyProvider) {
        checkMergePolicy(collectionConfig, mergePolicyProvider, collectionConfig.getMergePolicyConfig().getPolicy());
    }

    /**
     * Validates the given {@link RingbufferConfig}.
     *
     * @param ringbufferConfig    the {@link RingbufferConfig} to check
     * @param mergePolicyProvider the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkRingbufferConfig(RingbufferConfig ringbufferConfig,
                                             SplitBrainMergePolicyProvider mergePolicyProvider) {
        checkMergePolicy(ringbufferConfig, mergePolicyProvider, ringbufferConfig.getMergePolicyConfig().getPolicy());
    }

    /**
     * Validates the given {@link AbstractBasicConfig} implementation.
     *
     * @param basicConfig         the {@link AbstractBasicConfig} to check
     * @param mergePolicyProvider the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     * @param <C>                 type of the {@link AbstractBasicConfig}
     */
    public static <C extends AbstractBasicConfig> void checkBasicConfig(C basicConfig,
                                                                        SplitBrainMergePolicyProvider mergePolicyProvider) {
        checkMergePolicy(basicConfig, mergePolicyProvider, basicConfig.getMergePolicyConfig().getPolicy());
    }

    /**
     * Validates the given {@link ScheduledExecutorConfig}.
     *
     * @param scheduledExecutorConfig the {@link ScheduledExecutorConfig} to check
     * @param mergePolicyProvider     the {@link SplitBrainMergePolicyProvider} to resolve merge policy classes
     */
    public static void checkScheduledExecutorConfig(ScheduledExecutorConfig scheduledExecutorConfig,
                                                    SplitBrainMergePolicyProvider mergePolicyProvider) {
        String mergePolicyClassName = scheduledExecutorConfig.getMergePolicyConfig().getPolicy();
        checkMergePolicy(scheduledExecutorConfig, mergePolicyProvider, mergePolicyClassName);
    }

    public static void checkCPSubsystemConfig(CPSubsystemConfig config) {
        checkTrue(config.getGroupSize() <= config.getCPMemberCount(),
                "The group size parameter cannot be bigger than the number of the CP member count");

        checkTrue(config.getSessionTimeToLiveSeconds() > config.getSessionHeartbeatIntervalSeconds(),
                "Session TTL must be greater than session heartbeat interval!");

        checkTrue(config.getMissingCPMemberAutoRemovalSeconds() == 0
                        || config.getSessionTimeToLiveSeconds() <= config.getMissingCPMemberAutoRemovalSeconds(),
                "Session TTL must be smaller than or equal to missing CP member auto-removal seconds!");
    }

    /**
     * Throws {@link IllegalArgumentException} if the given {@link InMemoryFormat}
     * is {@link InMemoryFormat#NATIVE} and Hazelcast is OS.
     *
     * @param inMemoryFormat supplied inMemoryFormat
     */
    private static void checkNotNativeWhenOpenSource(InMemoryFormat inMemoryFormat) {
        if (inMemoryFormat == NATIVE && !getBuildInfo().isEnterprise()) {
            throw new IllegalArgumentException("NATIVE storage format is supported in Hazelcast Enterprise only."
                    + " Make sure you have Hazelcast Enterprise JARs on your classpath!");
        }
    }

    /**
     * Throws {@link IllegalArgumentException} if the given {@link NearCacheConfig}
     * has an invalid {@link NearCachePreloaderConfig}.
     *
     * @param nearCacheConfig supplied NearCacheConfig
     * @param isClient        {@code true} if the config is for a Hazelcast client, {@code false} otherwise
     */
    private static void checkPreloaderConfig(NearCacheConfig nearCacheConfig, boolean isClient) {
        if (!isClient && nearCacheConfig.getPreloaderConfig().isEnabled()) {
            throw new IllegalArgumentException("The Near Cache pre-loader is just available on Hazelcast clients!");
        }
    }

    /**
     * Throws {@link ConfigurationException} if given group property is defined within Hazelcast properties.
     *
     * @param properties        Group properties
     * @param hazelcastProperty property to be checked
     * @throws ConfigurationException
     */
    public static void ensurePropertyNotConfigured(HazelcastProperties properties, HazelcastProperty hazelcastProperty)
            throws ConfigurationException {
        if (properties.containsKey(hazelcastProperty)) {
            throw new ConfigurationException("Service start failed. The legacy property " + hazelcastProperty.getName()
                    + " is provided together with new Config object. "
                    + "Remove the property from your configuration to fix this issue.");
        }
    }

    /**
     * Checks if given group property is defined within given Hazelcast properties. Logs a warning when the property is defied.
     *
     * @return {@code true} when the property is defined
     */
    public static boolean checkAndLogPropertyDeprecated(HazelcastProperties properties, HazelcastProperty hazelcastProperty) {
        if (properties.containsKey(hazelcastProperty)) {
            LOGGER.warning(
                    "Property " + hazelcastProperty.getName() + " is deprecated. Use configuration object/element instead.");
            return properties.getBoolean(hazelcastProperty);
        }
        return false;
    }
}
