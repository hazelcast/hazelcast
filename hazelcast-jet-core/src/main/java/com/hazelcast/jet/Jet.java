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

package com.hazelcast.jet;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.matcher.MatchingPointConfigPatternMatcher;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.jet.config.JetClientConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.MetricsConfig;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.JetNodeContext;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.metrics.JetMetricsService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.merge.IgnoreMergingEntryMapMergePolicy;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.Properties;
import java.util.function.Function;

import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;
import static com.hazelcast.jet.impl.JobRepository.JOB_RESULTS_MAP_NAME;
import static com.hazelcast.jet.impl.config.ConfigProvider.locateAndGetClientConfig;
import static com.hazelcast.jet.impl.config.ConfigProvider.locateAndGetJetConfig;
import static com.hazelcast.jet.impl.metrics.JetMetricsService.applyMetricsConfig;
import static com.hazelcast.jet.impl.util.JetProperties.JET_SHUTDOWNHOOK_ENABLED;
import static com.hazelcast.jet.impl.util.JetProperties.JOB_RESULTS_TTL_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.SHUTDOWNHOOK_ENABLED;

/**
 * Entry point to the Jet product.
 */
public final class Jet {

    private static final ILogger LOGGER = Logger.getLogger(Jet.class);

    private Jet() {
    }

    /**
     * Creates a member of the Jet cluster with the configuration loaded from
     * default location.
     */
    @Nonnull
    public static JetInstance newJetInstance() {
        JetConfig config = locateAndGetJetConfig();
        return newJetInstance(config);
    }

    /**
     * Creates a member of the Jet cluster with the given configuration.
     */
    @Nonnull
    public static JetInstance newJetInstance(@Nonnull JetConfig config) {
        Preconditions.checkNotNull(config, "config");
        return newJetInstanceImpl(config, cfg ->
                HazelcastInstanceFactory.newHazelcastInstance(cfg, cfg.getInstanceName(), new JetNodeContext()));
    }

    /**
     * Creates a Jet client with the default configuration.
     */
    @Nonnull
    public static JetInstance newJetClient() {
        ClientConfig clientConfig = locateAndGetClientConfig();
        return newJetClient(clientConfig);
    }

    /**
     * Creates a Jet client with the given Hazelcast client configuration.
     * <p>
     * {@link JetClientConfig} may be used to create a configuration with the
     * default group name for Jet.
     */
    @Nonnull
    public static JetInstance newJetClient(@Nonnull ClientConfig config) {
        Preconditions.checkNotNull(config, "config");
        return getJetClientInstance(HazelcastClient.newHazelcastClient(config));
    }

    /**
     * Shuts down all running Jet client and member instances.
     */
    public static void shutdownAll() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    static JetInstance newJetInstanceImpl(JetConfig config, Function<Config, HazelcastInstance> newHzFn) {
        configureJetService(config);
        HazelcastInstanceImpl hzImpl = ((HazelcastInstanceProxy) newHzFn.apply(config.getHazelcastConfig()))
                .getOriginal();
        JetService jetService = hzImpl.node.nodeEngine.getService(JetService.SERVICE_NAME);
        return jetService.getJetInstance();
    }

    static JetClientInstanceImpl getJetClientInstance(HazelcastInstance client) {
        return new JetClientInstanceImpl(((HazelcastClientProxy) client).client);
    }

    private static synchronized void configureJetService(JetConfig jetConfig) {
        Config hzConfig = jetConfig.getHazelcastConfig();
        if (!(hzConfig.getConfigPatternMatcher() instanceof MatchingPointConfigPatternMatcher)) {
            throw new UnsupportedOperationException("Custom config pattern matcher is not supported in Jet");
        }

        Properties jetProps = jetConfig.getProperties();
        Properties hzProperties = hzConfig.getProperties();

        // Disable HZ shutdown hook, as we will use the Jet-specific property instead
        String hzHookEnabled = hzProperties.getProperty(
                SHUTDOWNHOOK_ENABLED.getName(), SHUTDOWNHOOK_ENABLED.getDefaultValue()
        );
        if (!jetProps.containsKey(JET_SHUTDOWNHOOK_ENABLED)) {
            jetProps.setProperty(JET_SHUTDOWNHOOK_ENABLED.getName(), hzHookEnabled);
        }
        hzConfig.setProperty(SHUTDOWNHOOK_ENABLED.getName(), "false");

        // copy Jet properties to HZ properties
        for (String prop : jetProps.stringPropertyNames()) {
            hzProperties.setProperty(prop, jetProps.getProperty(prop));
        }

        hzConfig.getServicesConfig()
                .addServiceConfig(new ServiceConfig()
                        // use the user service config for JetService only as a config object holder,
                        // the service will be created by JetNodeExtension
                        .setEnabled(false)
                        .setName(JetService.SERVICE_NAME)
                        .setClassName(JetService.class.getName())
                        .setConfigObject(jetConfig))
                .addServiceConfig(new ServiceConfig()
                        .setEnabled(true)
                        .setName(JetMetricsService.SERVICE_NAME)
                        .setClassName(JetMetricsService.class.getName())
                        .setConfigObject(jetConfig.getMetricsConfig()));

        MapConfig internalMapConfig = new MapConfig(INTERNAL_JET_OBJECTS_PREFIX + '*')
                .setBackupCount(jetConfig.getInstanceConfig().getBackupCount())
                // we query creationTime of resources maps
                .setStatisticsEnabled(true);

        internalMapConfig.getMergePolicyConfig().setPolicy(IgnoreMergingEntryMapMergePolicy.class.getName());

        HazelcastProperties properties = new HazelcastProperties(hzProperties);
        MapConfig resultsMapConfig = new MapConfig(internalMapConfig)
                .setName(JOB_RESULTS_MAP_NAME)
                .setTimeToLiveSeconds(properties.getSeconds(JOB_RESULTS_TTL_SECONDS));

        hzConfig.addMapConfig(internalMapConfig)
                .addMapConfig(resultsMapConfig);

        if (jetConfig.getInstanceConfig().isLosslessRestartEnabled() &&
            !hzConfig.getHotRestartPersistenceConfig().isEnabled()) {
            LOGGER.warning("Lossless recovery is enabled but Hot Restart is disabled. Auto-enabling Hot Restart. " +
                    "The following path will be used: " + hzConfig.getHotRestartPersistenceConfig().getBaseDir());
            hzConfig.getHotRestartPersistenceConfig().setEnabled(true);
        }

        MetricsConfig metricsConfig = jetConfig.getMetricsConfig();
        applyMetricsConfig(hzConfig, metricsConfig);
    }
}
