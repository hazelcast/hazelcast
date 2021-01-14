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

package com.hazelcast.jet.impl;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetCacheManager;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.util.ConcurrentMemoizingSupplier;
import com.hazelcast.jet.impl.util.JetConsoleLogHandler;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.topic.ITopic;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.jar.JarFile;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.stream.Collectors;

import static com.hazelcast.jet.core.JobStatus.NOT_RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.impl.config.ConfigProvider.locateAndGetJetConfig;
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.spi.properties.ClusterProperty.LOGGING_TYPE;

/**
 * This class shouldn't be directly used, instead see {@link Jet#bootstrappedInstance()}
 * for the replacement and docs.
 * <p>
 * A helper class that allows one to create a standalone runnable JAR which
 * contains all the code needed to submit a job to a running Jet cluster.
 * The main issue with achieving this is that the JAR must be attached as a
 * resource to the job being submitted, so the Jet cluster will be able to
 * load and use its classes. However, from within a running {@code main()}
 * method it is not trivial to find out the filename of the JAR containing
 * it.
 **/
public final class JetBootstrap {

    // supplier should be set only once
    private static ConcurrentMemoizingSupplier<BootstrappedJetProxy> supplier;

    private static final ILogger LOGGER = Logger.getLogger(Jet.class.getName());
    private static final AtomicBoolean LOGGING_CONFIGURED = new AtomicBoolean(false);
    private static final int JOB_START_CHECK_INTERVAL_MILLIS = 1_000;
    private static final EnumSet<JobStatus> STARTUP_STATUSES = EnumSet.of(NOT_RUNNING, STARTING);

    private JetBootstrap() {
    }

    public static synchronized void executeJar(@Nonnull Supplier<JetInstance> supplierOfJet,
                           @Nonnull String jar, @Nullable String snapshotName,
                           @Nullable String jobName, @Nullable String mainClass, @Nonnull List<String> args
    ) throws Exception {
        if (supplier != null) {
            throw new IllegalStateException(
                    "Supplier of JetInstance was already set. This method should not" +
                    " be called outside the Jet command line.");
        }

        supplier = new ConcurrentMemoizingSupplier<>(() ->
                new BootstrappedJetProxy(supplierOfJet.get(), jar, snapshotName, jobName)
        );
        try (JarFile jarFile = new JarFile(jar)) {
            if (StringUtil.isNullOrEmpty(mainClass)) {
                if (jarFile.getManifest() == null) {
                    error("No manifest file in " + jar + ". You can use the -c option to provide the main class.");
                }
                mainClass = jarFile.getManifest().getMainAttributes().getValue("Main-Class");
                if (mainClass == null) {
                    error("No Main-Class found in manifest. You can use the -c option to provide the main class.");
                }
            }

            URL jarUrl = new URL("file:///" + jar);
            URLClassLoader classLoader = AccessController.doPrivileged(
                    (PrivilegedAction<URLClassLoader>) () ->
                            new URLClassLoader(new URL[]{jarUrl}, JetBootstrap.class.getClassLoader())
            );

            Class<?> clazz = loadMainClass(classLoader, mainClass);

            Method main = clazz.getDeclaredMethod("main", String[].class);
            int mods = main.getModifiers();
            if ((mods & Modifier.PUBLIC) == 0 || (mods & Modifier.STATIC) == 0) {
                error("Class " + clazz.getName()
                        + " has a main(String[] args) method which is not public static");
            }
            String[] jobArgs = args.toArray(new String[0]);
            // upcast args to Object so it's passed as a single array-typed argument
            main.invoke(null, (Object) jobArgs);
            awaitJobsStarted();
        } finally {
            JetInstance remembered = JetBootstrap.supplier.remembered();
            if (remembered != null) {
                try {
                    remembered.shutdown();
                } catch (Throwable t) {
                    System.err.println("Shutdown failed with:");
                    t.printStackTrace();
                }
            }
            JetBootstrap.supplier = null;
        }
    }

    private static void awaitJobsStarted() {
        List<Job> submittedJobs = JetBootstrap.supplier.get().submittedJobs();
        int submittedCount = submittedJobs.size();
        if (submittedCount == 0) {
            System.out.println("The JAR didn't submit any jobs.");
            return;
        }
        int previousCount = -1;
        while (true) {
            uncheckRun(() -> Thread.sleep(JOB_START_CHECK_INTERVAL_MILLIS));
            List<Job> startedJobs = submittedJobs.stream()
                .filter(job -> !STARTUP_STATUSES.contains(job.getStatus()))
                .collect(Collectors.toList());

            submittedJobs = submittedJobs.stream()
                    .filter(job -> !startedJobs.contains(job))
                    .collect(Collectors.toList());

            int remainingCount = submittedJobs.size();

            if (submittedJobs.isEmpty() && remainingCount == previousCount) {
                break;
            }
            if (remainingCount == previousCount) {
                continue;
            }
            for (Job job : startedJobs) {
                // The change of job statuses after the check above
                // won't be a problem here. Because they cannot revert
                // back to startup statuses.
                if (job.getName() != null) {
                    System.out.println("Job '" + job.getName() + "' submitted at "
                            + toLocalDateTime(job.getSubmissionTime()) + " changed status to "
                            + job.getStatus() + " at " + toLocalDateTime(System.currentTimeMillis()) + ".");
                } else {
                    System.out.println("Job '" + job.getIdString() + "' submitted at "
                            + toLocalDateTime(job.getSubmissionTime())  + " changed status to "
                            + job.getStatus() + " at " + toLocalDateTime(System.currentTimeMillis()) + ".");
                }
            }
            if (remainingCount == 1) {
                System.out.println("A job is still starting...");
            } else if (remainingCount > 1) {
                System.out.format("%,d jobs are still starting...%n", remainingCount);
            }
            previousCount = remainingCount;
        }
    }

    private static Class<?> loadMainClass(ClassLoader classLoader, String mainClass) throws ClassNotFoundException {
        try {
            return classLoader.loadClass(mainClass);
        } catch (ClassNotFoundException e) {
            System.err.println("Cannot find or load main class: " + mainClass);
            throw e;
        }
    }

    private static void error(String msg) {
        System.err.println(msg);
        System.exit(1);
    }

    /**
     * Returns the bootstrapped {@code JetInstance}. The instance will be
     * automatically shut down once the {@code main()} method of the JAR returns.
     */
    @Nonnull
    public static synchronized JetInstance getInstance() {
        if (supplier == null) {
            supplier = new ConcurrentMemoizingSupplier<>(() -> new BootstrappedJetProxy(createStandaloneInstance()));
        }
        return supplier.get();
    }

    private static JetInstance createStandaloneInstance() {
        configureLogging();
        LOGGER.info("Bootstrapped instance requested but application wasn't called from jet submit script. " +
                "Creating a standalone Jet instance instead.");
        JetConfig config = locateAndGetJetConfig();
        Config hzConfig = config.getHazelcastConfig();

        // turn off all discovery to make sure node doesn't join any existing cluster
        hzConfig.setProperty("hazelcast.wait.seconds.before.join", "0");
        hzConfig.getAdvancedNetworkConfig().setEnabled(false);

        JoinConfig join = hzConfig.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAwsConfig().setEnabled(false);
        join.getGcpConfig().setEnabled(false);
        join.getAzureConfig().setEnabled(false);
        join.getKubernetesConfig().setEnabled(false);
        join.getEurekaConfig().setEnabled(false);
        join.setDiscoveryConfig(new DiscoveryConfig());
        return Jet.newJetInstance(config);
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
                System.err.println("Error configuring java.util.logging for Jet: " + e);
            }
        }
    }

    private static class BootstrappedJetProxy extends AbstractJetInstance {
        private final AbstractJetInstance instance;
        private final String jar;
        private final String snapshotName;
        private final String jobName;
        private final Collection<Job> submittedJobs = new CopyOnWriteArrayList<>();

        BootstrappedJetProxy(@Nonnull JetInstance hazelcastInstance) {
            this(hazelcastInstance, null, null, null);
        }

        BootstrappedJetProxy(
                @Nonnull JetInstance instance,
                @Nullable String jar,
                @Nullable String snapshotName,
                @Nullable String jobName
        ) {
            super(instance.getHazelcastInstance());
            this.instance = (AbstractJetInstance) instance;
            this.jar = jar;
            this.snapshotName = snapshotName;
            this.jobName = jobName;
        }

        @Nonnull @Override
        public String getName() {
            return instance.getName();
        }

        @Nonnull @Override
        public HazelcastInstance getHazelcastInstance() {
            return instance.getHazelcastInstance();
        }

        @Nonnull @Override
        public Cluster getCluster() {
            return instance.getCluster();
        }

        @Nonnull @Override
        public JetConfig getConfig() {
            return instance.getConfig();
        }

        @Nonnull @Override
        public Job newJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
            return remember(instance.newJob(pipeline, updateJobConfig(config)));
        }

        @Nonnull @Override
        public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
            return remember(instance.newJob(dag, updateJobConfig(config)));
        }

        @Nonnull @Override
        public Job newJobIfAbsent(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
            return remember(instance.newJobIfAbsent(pipeline, updateJobConfig(config)));
        }

        @Nonnull @Override
        public Job newJobIfAbsent(@Nonnull DAG dag, @Nonnull JobConfig config) {
            return remember(instance.newJobIfAbsent(dag, updateJobConfig(config)));
        }

        List<Job> submittedJobs() {
            return new ArrayList<>(submittedJobs);
        }

        private Job remember(@Nonnull Job job) {
            submittedJobs.add(job);
            return job;
        }

        private JobConfig updateJobConfig(@Nonnull JobConfig config) {
            if (jar != null) {
                config.addJar(jar);
            }
            if (snapshotName != null) {
                config.setInitialSnapshotName(snapshotName);
            }
            if (jobName != null) {
                config.setName(jobName);
            }
            return config;
        }

        @Nonnull @Override
        public List<Job> getJobs() {
            return instance.getJobs();
        }

        @Override
        public Job getJob(long jobId) {
            return instance.getJob(jobId);
        }

        @Nonnull @Override
        public List<Job> getJobs(@Nonnull String name) {
            return instance.getJobs(name);
        }

        @Nonnull @Override
        public <K, V> IMap<K, V> getMap(@Nonnull String name) {
            return instance.getMap(name);
        }

        @Nonnull @Override
        public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
            return instance.getReplicatedMap(name);
        }

        @Nonnull @Override
        public JetCacheManager getCacheManager() {
            return instance.getCacheManager();
        }

        @Nonnull @Override
        public <E> IList<E> getList(@Nonnull String name) {
            return instance.getList(name);
        }

        @Nonnull @Override
        public <T> ITopic<T> getReliableTopic(@Nonnull String name) {
            return instance.getReliableTopic(name);
        }

        @Nonnull @Override
        public <T> Observable<T> getObservable(@Nonnull String name) {
            return instance.getObservable(name);
        }

        @Override
        public void shutdown() {
            instance.shutdown();
        }

        @Override
        public boolean existsDistributedObject(@Nonnull String serviceName, @Nonnull String objectName) {
            return instance.existsDistributedObject(serviceName, objectName);
        }

        @Override
        public ILogger getLogger() {
            return instance.getLogger();
        }

        @Override
        public Job newJobProxy(long jobId) {
            return instance.newJobProxy(jobId);
        }

        @Override
        public Job newJobProxy(long jobId, Object jobDefinition, JobConfig config) {
            return instance.newJobProxy(jobId, jobDefinition, config);
        }

        @Override
        public List<Long> getJobIdsByName(String name) {
            return instance.getJobIdsByName(name);
        }
    }
}
