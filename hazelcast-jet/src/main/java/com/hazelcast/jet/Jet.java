/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.matcher.MatchingPointConfigPatternMatcher;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.config.JetClientConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JetBootstrap;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.JetNodeContext;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

import static com.hazelcast.jet.core.JetProperties.JET_IMDG_VERSION_CHECK_DISABLED;
import static com.hazelcast.jet.core.JetProperties.JET_SHUTDOWNHOOK_ENABLED;
import static com.hazelcast.jet.core.JetProperties.JOB_RESULTS_TTL_SECONDS;
import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;
import static com.hazelcast.jet.impl.JobRepository.JOB_METRICS_MAP_NAME;
import static com.hazelcast.jet.impl.JobRepository.JOB_RESULTS_MAP_NAME;
import static com.hazelcast.jet.impl.config.ConfigProvider.locateAndGetClientConfig;
import static com.hazelcast.jet.impl.config.ConfigProvider.locateAndGetJetConfig;
import static com.hazelcast.spi.properties.ClusterProperty.SHUTDOWNHOOK_ENABLED;
import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getProperty;

/**
 * Entry point to the Jet product.
 *
 * @since 3.0
 */
public final class Jet {

    private static final ILogger LOGGER = Logger.getLogger(Jet.class);

    static {
        JetBootstrap.configureLogging();
        assertHazelcastVersion();
    }

    private Jet() {
    }

    /**
     * Returns either a local Jet instance or a "bootstrapped" Jet client for
     * a remote Jet cluster, depending on the context. The main goal of this
     * factory method is to simplify submitting a Jet job to a remote cluster
     * while also making it convenient to test on the local machine.
     * <p>
     * When you submit a job to a Jet instance that runs locally in your JVM,
     * it will have all the dependency classes available. However, when you
     * take the same job to a remote Jet cluster, you'll often find that it
     * fails with a {@code ClassNotFoundException} because the remote cluster
     * doesn't have all the classes you use in the job.
     * <p>
     * Normally you would have to explicitly add all the dependency classes to
     * the {@link JobConfig#addClass(Class[]) JobConfig}, either one by one or
     * packaged into a JAR. If you're submitting a job using the command-line
     * tool {@code jet submit}, the JAR to attach is the same JAR that
     * contains the code that submits the job.
     * <p>
     * This factory takes all of the above into account in order to provide a
     * smoother experience:
     * <ul><li>
     *     When not called from {@code jet submit}, it returns a local {@code
     *     JetInstance}, either by creating a new one or looking up a cached one.
     *     The instance won't join any cluster.
     * <li>
     *     When called from {@code jet submit}, it returns a "bootstrapped"
     *     instance of Jet client that automatically attaches the JAR to all jobs
     *     you submit using it.
     * </ul>
     * With these semantics in place it's simple to write code that works both
     * in your local development/testing environment (using a local Jet
     * instance) and in production (using the remote cluster).
     * <p>
     * To use this feature, follow these steps:
     * <ol><li>
     *     Write your {@code main()} method and your Jet code the usual way, making
     *     sure you use this method (instead of {@link #newJetClient()}) to acquire
     *     a Jet client instance.
     * <li>
     *     Create a runnable JAR (e.g. {@code jetjob.jar}) with your entry point
     *     declared as the {@code Main-Class} in {@code MANIFEST.MF}. The JAR should
     *     include all dependencies required to run it (except the Jet classes
     *     &mdash; these are already available on the cluster classpath).
     * <li>
     *     Submit the job by writing {@code jet submit jetjob.jar} on the command
     *     line. This assumes you have downloaded the Jet distribution package and
     *     its {@code bin} directory is on your system path. The Jet client will use
     *     the configuration file {@code <distro_root>/config/hazelcast-client.yaml}.
     *     Adjust that file as needed.
     * <li>
     *     The same code will work if you run it directly from your IDE. In this
     *     case it will create a local Jet instance for itself to run on.
     * </ol>
     * For example, you can write a class like this:
     * <pre>
     * public class CustomJetJob {
     *   public static void main(String[] args) {
     *     JetInstance jet = Jet.bootstrappedInstance();
     *     jet.newJob(buildPipeline()).join();
     *   }
     *
     *   public static Pipeline createPipeline() {
     *       // ...
     *   }
     * }
     * </pre>
     *
     * @since 4.0
     */
    @Nonnull
    public static JetInstance bootstrappedInstance() {
        return JetBootstrap.getInstance();
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
     * Creates a Jet client with cluster failover capability. Client will try to connect
     * to alternative clusters according to the supplied {@link ClientFailoverConfig}
     * when it disconnects from a cluster.
     */
    @Nonnull
    public static JetInstance newJetFailoverClient(@Nonnull ClientFailoverConfig config) {
        Preconditions.checkNotNull(config, "config");
        return getJetClientInstance(HazelcastClient.newHazelcastFailoverClient(config));
    }

    /**
     * Creates a Jet client with cluster failover capability. The client will
     * try to connect to alternative clusters as specified in the resolved {@link
     * ClientFailoverConfig} when it disconnects from a cluster.
     * <p>
     * The failover configuration is loaded using the following resolution mechanism:
     * <ol>
     * <li>System property {@code hazelcast.client.failover.config} is checked. If found,
     * and begins with {@code classpath:}, then a classpath resource is loaded, otherwise
     * it will be loaded from the file system. The configuration can be either an XML or a YAML
     * file, distinguished by the suffix of the provided file</li>
     * <li>{@code hazelcast-client-failover.xml} is checked on in the working dir</li>
     * <li>{@code hazelcast-client-failover.xml} is checked on the classpath</li>
     * <li>{@code hazelcast-client-failover.yaml} is checked on the working dir</li>
     * <li>{@code hazelcast-client-failover.yaml} is checked on the classpath</li>
     * <li>If none are available, then a {@link HazelcastException} is thrown</li>
     * </ol>
     */
    @Nonnull
    public static JetInstance newJetFailoverClient() {
        return getJetClientInstance(HazelcastClient.newHazelcastFailoverClient());
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

    /**
     * Makes sure that the Jet-expected Hazelcast version and the one in classpath match
     */
    private static void assertHazelcastVersion() {
        String hzVersion = BuildInfoProvider.getBuildInfo().getVersion();
        try (InputStream resource = Jet.class.getClassLoader().getResourceAsStream("jet-runtime.properties")) {
            Properties p = new Properties();
            p.load(resource);
            String jetHzVersion = p.getProperty("jet.hazelcast.version");

            if (!hzVersion.equals(jetHzVersion)) {
                String message = "Jet uses Hazelcast IMDG version " + jetHzVersion + " however " +
                        "version " + hzVersion + " was found in the classpath. " +
                        " As Jet already shades Hazelcast jars there is no need to explicitly " +
                        "add a dependency to it.";
                boolean errorOnMismatch = !versionCheckDisabled();
                if (errorOnMismatch) {
                    throw new JetException(message);
                } else {
                    LOGGER.warning(message);
                }
            }
        } catch (IOException e) {
            LOGGER.warning("Could not read the file jet-runtime.properties", e);
        }
    }

    private static boolean versionCheckDisabled() {
        String rawValue = getProperty(JET_IMDG_VERSION_CHECK_DISABLED.getName(),
                JET_IMDG_VERSION_CHECK_DISABLED.getDefaultValue());
        return parseBoolean(rawValue);
    }

    private static synchronized void configureJetService(JetConfig jetConfig) {
        Config hzConfig = jetConfig.getHazelcastConfig();
        if (!(hzConfig.getConfigPatternMatcher() instanceof MatchingPointConfigPatternMatcher)) {
            throw new UnsupportedOperationException("Custom config pattern matcher is not supported in Jet");
        }

        Properties jetProps = jetConfig.getProperties();
        Properties hzProperties = hzConfig.getProperties();

        // Disable HZ shutdown hook, as we will use the Jet-specific property instead
        String hzHookEnabled = Optional.ofNullable(hzConfig.getProperty(SHUTDOWNHOOK_ENABLED.getName()))
                .orElse(SHUTDOWNHOOK_ENABLED.getDefaultValue());

        if (!jetProps.containsKey(JET_SHUTDOWNHOOK_ENABLED)) {
            jetProps.setProperty(JET_SHUTDOWNHOOK_ENABLED.getName(), hzHookEnabled);
        }

        // this property should behave as if false is the default
        HazelcastProperty loggingDetails = ClusterProperty.LOGGING_ENABLE_DETAILS;
        if (loggingDetails.getSystemProperty() == null
                && !jetProps.containsKey(loggingDetails)
                && !hzProperties.containsKey(loggingDetails)
        ) {
            jetProps.setProperty(loggingDetails.getName(), "false");
        }

        hzConfig.setProperty(SHUTDOWNHOOK_ENABLED.getName(), "false");

        // copy Jet properties to HZ properties
        for (String prop : jetProps.stringPropertyNames()) {
            hzProperties.setProperty(prop, jetProps.getProperty(prop));
        }

        ConfigAccessor.getServicesConfig(hzConfig)
                      .addServiceConfig(new ServiceConfig()
                              // use the user service config for JetService only as a config object holder,
                              // the service will be created by JetNodeExtension
                              .setEnabled(false)
                              .setName(JetService.SERVICE_NAME)
                              .setClassName(JetService.class.getName())
                              .setConfigObject(jetConfig));

        MapConfig internalMapConfig = new MapConfig(INTERNAL_JET_OBJECTS_PREFIX + '*')
                .setBackupCount(jetConfig.getInstanceConfig().getBackupCount())
                // we query creationTime of resources maps
                .setStatisticsEnabled(true);

        internalMapConfig.getMergePolicyConfig().setPolicy(DiscardMergePolicy.class.getName());

        HazelcastProperties properties = new HazelcastProperties(hzProperties);
        MapConfig resultsMapConfig = new MapConfig(internalMapConfig)
                .setName(JOB_RESULTS_MAP_NAME)
                .setTimeToLiveSeconds(properties.getSeconds(JOB_RESULTS_TTL_SECONDS));

        MapConfig metricsMapConfig = new MapConfig(internalMapConfig)
            .setName(JOB_METRICS_MAP_NAME)
            .setTimeToLiveSeconds(properties.getSeconds(JOB_RESULTS_TTL_SECONDS));

        hzConfig.addMapConfig(internalMapConfig)
                .addMapConfig(resultsMapConfig)
                .addMapConfig(metricsMapConfig);

        if (jetConfig.getInstanceConfig().isLosslessRestartEnabled() &&
            !hzConfig.getHotRestartPersistenceConfig().isEnabled()) {
            LOGGER.warning("Lossless Restart is enabled but Hot Restart is disabled. Auto-enabling Hot Restart. " +
                    "The following path will be used: " + hzConfig.getHotRestartPersistenceConfig().getBaseDir());
            hzConfig.getHotRestartPersistenceConfig().setEnabled(true);
        }
    }
}
