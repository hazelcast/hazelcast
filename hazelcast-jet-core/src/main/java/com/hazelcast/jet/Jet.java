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
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.jet.config.JetClientConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.MetricsConfig;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.metrics.JetMetricsService;
import com.hazelcast.map.merge.IgnoreMergingEntryMapMergePolicy;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.Properties;
import java.util.function.Function;

import static com.hazelcast.jet.impl.JobRepository.JOB_RESULTS_MAP_NAME;
import static com.hazelcast.jet.impl.config.XmlJetConfigBuilder.getClientConfig;
import static com.hazelcast.jet.impl.metrics.JetMetricsService.applyMetricsConfig;
import static com.hazelcast.jet.impl.util.JetGroupProperty.JOB_RESULTS_TTL_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.SHUTDOWNHOOK_ENABLED;

/**
 * Entry point to the Jet product.
 */
public final class Jet {

    /**
     * Prefix of all Hazelcast internal objects used by Jet (such as job
     * metadata, snapshots etc.)
     */
    public static final String INTERNAL_JET_OBJECTS_PREFIX = "__jet.";

    private Jet() {
    }

    /**
     * Creates a member of the Jet cluster with the given configuration.
     */
    public static JetInstance newJetInstance(JetConfig config) {
        return newJetInstanceImpl(config, Hazelcast::newHazelcastInstance);
    }

    /**
     * Creates a member of the Jet cluster with the configuration loaded from
     * default location.
     */
    public static JetInstance newJetInstance() {
        return newJetInstance(JetConfig.loadDefault());
    }

    /**
     * Creates a Jet client with the default configuration.
     */
    public static JetInstance newJetClient() {
        ClientConfig clientConfig = getClientConfig();
        return newJetClient(clientConfig);
    }

    /**
     * Creates a Jet client with the given Hazelcast client configuration.
     *
     * {@link JetClientConfig} may be used to create a configuration with the
     * default group name and password for Jet.
     */
    public static JetInstance newJetClient(ClientConfig config) {
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
        jetService.getJobCoordinationService().startScanningForJobs();
        return jetService.getJetInstance();
    }

    static JetClientInstanceImpl getJetClientInstance(HazelcastInstance client) {
        return new JetClientInstanceImpl(((HazelcastClientProxy) client).client);
    }

    static void configureJetService(JetConfig jetConfig) {
        Config hzConfig = jetConfig.getHazelcastConfig();
        if (!(hzConfig.getConfigPatternMatcher() instanceof MatchingPointConfigPatternMatcher)) {
            throw new UnsupportedOperationException("Custom config pattern matcher is not supported in Jet");
        }

        Properties jetProps = jetConfig.getProperties();
        Properties hzProperties = hzConfig.getProperties();

        // copy Jet Config properties as HZ properties
        for (String prop : jetProps.stringPropertyNames()) {
            hzProperties.setProperty(prop, jetProps.getProperty(prop));
        }

        HazelcastProperties properties = new HazelcastProperties(hzProperties);

        hzConfig.getServicesConfig()
                .addServiceConfig(new ServiceConfig()
                        .setEnabled(true)
                        .setName(JetService.SERVICE_NAME)
                        .setClassName(JetService.class.getName())
                        .setProperties(jetServiceProperties(properties))
                        .setConfigObject(jetConfig))
                .addServiceConfig(new ServiceConfig()
                        .setEnabled(true)
                        .setName(JetMetricsService.SERVICE_NAME)
                        .setClassName(JetMetricsService.class.getName())
                        .setConfigObject(jetConfig.getMetricsConfig()));

        boolean hotRestartEnabled = hzConfig.getHotRestartPersistenceConfig().isEnabled();
        MapConfig metadataMapConfig = new MapConfig(INTERNAL_JET_OBJECTS_PREFIX + '*')
                .setBackupCount(jetConfig.getInstanceConfig().getBackupCount())
                .setStatisticsEnabled(false);
        metadataMapConfig.getMergePolicyConfig().setPolicy(IgnoreMergingEntryMapMergePolicy.class.getName());
        metadataMapConfig.getHotRestartConfig().setEnabled(hotRestartEnabled);

        MapConfig resultsMapConfig = new MapConfig(metadataMapConfig)
                .setName(JOB_RESULTS_MAP_NAME)
                .setTimeToLiveSeconds(properties.getSeconds(JOB_RESULTS_TTL_SECONDS));
        resultsMapConfig.getHotRestartConfig().setEnabled(hotRestartEnabled);

        hzConfig.addMapConfig(metadataMapConfig)
                .addMapConfig(resultsMapConfig);

        MetricsConfig metricsConfig = jetConfig.getMetricsConfig();
        applyMetricsConfig(hzConfig, metricsConfig);

        // Force disable IMDG shutdown hook, we will use the Jet property instead
        hzConfig.setProperty(SHUTDOWNHOOK_ENABLED.getName(), "false");
    }

    private static Properties jetServiceProperties(HazelcastProperties hzProperties) {
        Properties properties = new Properties();
        properties.setProperty(SHUTDOWNHOOK_ENABLED.getName(), hzProperties.getString(SHUTDOWNHOOK_ENABLED));
        return properties;
    }
}
