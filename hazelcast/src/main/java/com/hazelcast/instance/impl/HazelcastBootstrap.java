/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.executejar.ClientExecuteJarStrategy;
import com.hazelcast.instance.impl.executejar.ExecuteJobParameters;
import com.hazelcast.instance.impl.executejar.MemberExecuteJarStrategy;
import com.hazelcast.jet.impl.util.JetConsoleLogHandler;
import com.hazelcast.instance.impl.executejar.ResettableSingleton;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.properties.ClusterProperty;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

import static com.hazelcast.spi.properties.ClusterProperty.LOGGING_TYPE;

/**
 * This class shouldn't be directly used, instead see {@link Hazelcast#bootstrappedInstance()}
 * for the replacement and docs.
 * <p>
 * A helper class that allows one to create a standalone runnable JAR which
 * contains all the code needed to submit a job to a running Hazelcast cluster.
 * The main issue with achieving this is that the JAR must be attached as a
 * resource to the job being submitted, so the Hazelcast cluster will be able
 * to load and use its classes. However, from within a running {@code main()}
 * method it is not trivial to find out the filename of the JAR containing
 * it.
 **/
public final class HazelcastBootstrap {

    private static final ResettableSingleton<BootstrappedInstanceProxy> SINGLETON = new ResettableSingleton<>();

    private static final ClientExecuteJarStrategy CLIENT_EXECUTE_JAR_STRATEGY = new ClientExecuteJarStrategy();

    private static final MemberExecuteJarStrategy MEMBER_EXECUTE_JAR_STRATEGY = new MemberExecuteJarStrategy();

    private static final ILogger LOGGER = Logger.getLogger(HazelcastBootstrap.class);

    private static final AtomicBoolean LOGGING_CONFIGURED = new AtomicBoolean(false);

    private HazelcastBootstrap() {
    }

    // Public for testing
    public static void resetRemembered() {
        SINGLETON.resetRemembered();
    }

    /**
     * Execute jar file that exist on the client
     */
    public static void executeJarOnClient(@Nonnull Supplier<HazelcastInstance> supplierOfInstance,
                                          @Nonnull String jarPath,
                                          @Nullable String snapshotName,
                                          @Nullable String jobName,
                                          @Nullable String mainClassName,
                                          @Nonnull List<String> args)
            throws IOException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
        // Set the singleton, so that it can be accessed within the jar
        SINGLETON.get(() -> BootstrappedInstanceProxy.createWithJetProxy(supplierOfInstance.get()));

        ExecuteJobParameters executeJobParameters = new ExecuteJobParameters(jarPath, snapshotName, jobName);
        CLIENT_EXECUTE_JAR_STRATEGY.executeJar(SINGLETON, executeJobParameters, mainClassName, args);
    }

    /**
     * Execute jar file that exist on the member
     */
    public static void executeJarOnMember(@Nonnull Supplier<HazelcastInstance> supplierOfInstance,
                                          @Nonnull String jarPath,
                                          @Nullable String snapshotName,
                                          @Nullable String jobName,
                                          @Nullable String mainClassName,
                                          @Nonnull List<String> args)
            throws IOException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
        // Set the singleton, so that it can be accessed within the jar
        BootstrappedInstanceProxy hazelcastInstance =
                SINGLETON.get(() -> BootstrappedInstanceProxy.createWithJetProxy(supplierOfInstance.get()));

        ExecuteJobParameters executeJobParameters = new ExecuteJobParameters(jarPath, snapshotName, jobName);

        MEMBER_EXECUTE_JAR_STRATEGY.executeJar(hazelcastInstance, executeJobParameters, mainClassName, args);
    }

    /**
     * Returns the bootstrapped {@code HazelcastInstance}. The instance will be
     * automatically shut down once the {@code main()} method of the JAR returns.
     */
    @Nonnull
    public static synchronized HazelcastInstance getInstance() {
        return SINGLETON.get(() -> BootstrappedInstanceProxy.createWithJetProxy(createStandaloneInstance()));
    }

    private static HazelcastInstance createStandaloneInstance() {
        configureLogging();
        LOGGER.info("Bootstrapped instance requested but application wasn't called from hazelcast submit script. "
                    + "Creating a standalone Hazelcast instance instead. Jet is enabled in this standalone instance.");
        Config config = Config.load();
        // enable jet
        config.getJetConfig().setEnabled(true);

        // Disable Hazelcast from binding to all local network interfaces
        config.setProperty(ClusterProperty.SOCKET_BIND_ANY.getName(), "false");
        // Enable the interfaces approach for binding, and add localhost to available interfaces to bind
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");

        // turn off all discovery to make sure node doesn't join any existing cluster
        config.setProperty("hazelcast.wait.seconds.before.join", "0");
        config.getAdvancedNetworkConfig().setEnabled(false);

        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAwsConfig().setEnabled(false);
        join.getGcpConfig().setEnabled(false);
        join.getAzureConfig().setEnabled(false);
        join.getKubernetesConfig().setEnabled(false);
        join.getEurekaConfig().setEnabled(false);
        join.setDiscoveryConfig(new DiscoveryConfig());
        return Hazelcast.newHazelcastInstance(config);
    }

    public static void configureLogging() {
        if (LOGGING_CONFIGURED.compareAndSet(false, true)) {
            try {
                String loggingType = System.getProperty(LOGGING_TYPE.getName(), "jdk");
                if (loggingType.equals("jdk")) {
                    java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
                    for (Handler handler : rootLogger.getHandlers()) {
                        if (handler instanceof ConsoleHandler) {
                            rootLogger.removeHandler(handler);
                            rootLogger.addHandler(new JetConsoleLogHandler());
                            rootLogger.setLevel(Level.INFO);
                            return;
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.severe("Error configuring java.util.logging for Hazelcast: " + e);
            }
        }
    }
}
