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

package com.hazelcast.instance.impl;

import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.client.ClientService;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Endpoint;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICacheManager;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.jet.JetCacheManager;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.AbstractJetInstance;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation.GetJobIdsResult;
import com.hazelcast.jet.impl.util.ConcurrentMemoizingSupplier;
import com.hazelcast.jet.impl.util.JetConsoleLogHandler;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionService;
import com.hazelcast.sql.SqlService;
import com.hazelcast.topic.ITopic;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;

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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
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
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
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

    // supplier should be set only once
    private static ConcurrentMemoizingSupplier<BootstrappedInstanceProxy> supplier;

    private static final ILogger LOGGER = Logger.getLogger(Hazelcast.class.getName());
    private static final AtomicBoolean LOGGING_CONFIGURED = new AtomicBoolean(false);
    private static final int JOB_START_CHECK_INTERVAL_MILLIS = 1_000;
    private static final EnumSet<JobStatus> STARTUP_STATUSES = EnumSet.of(NOT_RUNNING, STARTING);

    private HazelcastBootstrap() {
    }

    public static synchronized void executeJar(@Nonnull Supplier<HazelcastInstance> supplierOfInstance,
                                               @Nonnull String jar, @Nullable String snapshotName,
                                               @Nullable String jobName, @Nullable String mainClass, @Nonnull List<String> args
    ) throws Exception {
        if (supplier != null) {
            throw new IllegalStateException(
                    "Supplier of HazelcastInstance was already set. This method should not"
                            + " be called outside the Hazelcast command line.");
        }

        supplier = new ConcurrentMemoizingSupplier<>(() ->
                new BootstrappedInstanceProxy(supplierOfInstance.get(), jar, snapshotName, jobName)
        );
        try (JarFile jarFile = new JarFile(jar)) {
            checkHazelcastCodebasePresence(jarFile);
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
                            new URLClassLoader(new URL[]{jarUrl}, HazelcastBootstrap.class.getClassLoader())
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
            HazelcastInstance remembered = HazelcastBootstrap.supplier.remembered();
            if (remembered != null) {
                try {
                    remembered.shutdown();
                } catch (Throwable t) {
                    System.err.println("Shutdown failed with:");
                    t.printStackTrace();
                }
            }
            HazelcastBootstrap.supplier = null;
        }
    }

    private static void checkHazelcastCodebasePresence(JarFile jarFile) {
        List<String> classFiles = JarScanner.findClassFiles(jarFile, HazelcastBootstrap.class.getSimpleName());
        if (!classFiles.isEmpty()) {
            System.err.format("WARNING: Hazelcast code detected in the jar: %s. "
                            + "Hazelcast dependency should be set with the 'provided' scope or equivalent.%n",
                    String.join(", ", classFiles));
        }
    }

    private static void awaitJobsStarted() {
        List<Job> submittedJobs = ((BootstrappedJetProxy) HazelcastBootstrap.supplier.get().getJet()).submittedJobs();
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
     * Returns the bootstrapped {@code HazelcastInstance}. The instance will be
     * automatically shut down once the {@code main()} method of the JAR returns.
     */
    @Nonnull
    public static synchronized HazelcastInstance getInstance() {
        if (supplier == null) {
            supplier = new ConcurrentMemoizingSupplier<>(() -> new BootstrappedInstanceProxy(createStandaloneInstance()));
        }
        return supplier.get();
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
                System.err.println("Error configuring java.util.logging for Hazelcast: " + e);
            }
        }
    }

    @SuppressWarnings({"checkstyle:methodcount"})
    private static class BootstrappedInstanceProxy implements HazelcastInstance {
        private final HazelcastInstance instance;
        private final JetService jetProxy;

        BootstrappedInstanceProxy(@Nonnull HazelcastInstance instance) {
            this(instance, null, null, null);
        }

        BootstrappedInstanceProxy(
                @Nonnull HazelcastInstance instance,
                @Nullable String jar,
                @Nullable String snapshotName,
                @Nullable String jobName
        ) {
            this.instance = instance;
            this.jetProxy = new BootstrappedJetProxy(instance.getJet(), jar, snapshotName, jobName);
        }

        @Nonnull
        @Override
        public String getName() {
            return instance.getName();
        }

        @Nonnull
        @Override
        public <K, V> IMap<K, V> getMap(@Nonnull String name) {
            return instance.getMap(name);
        }

        @Nonnull
        @Override
        public <E> IQueue<E> getQueue(@Nonnull String name) {
            return instance.getQueue(name);
        }

        @Nonnull
        @Override
        public <E> ITopic<E> getTopic(@Nonnull String name) {
            return instance.getTopic(name);
        }

        @Nonnull
        @Override
        public <E> ITopic<E> getReliableTopic(@Nonnull String name) {
            return instance.getReliableTopic(name);
        }

        @Nonnull
        @Override
        public <E> ISet<E> getSet(@Nonnull String name) {
            return instance.getSet(name);
        }

        @Nonnull
        @Override
        public <E> IList<E> getList(@Nonnull String name) {
            return instance.getList(name);
        }

        @Nonnull
        @Override
        public <K, V> MultiMap<K, V> getMultiMap(@Nonnull String name) {
            return instance.getMultiMap(name);
        }

        @Nonnull
        @Override
        public <E> Ringbuffer<E> getRingbuffer(@Nonnull String name) {
            return instance.getRingbuffer(name);
        }

        @Nonnull
        @Override
        public IExecutorService getExecutorService(@Nonnull String name) {
            return instance.getExecutorService(name);
        }

        @Nonnull
        @Override
        public DurableExecutorService getDurableExecutorService(@Nonnull String name) {
            return instance.getDurableExecutorService(name);
        }

        @Override
        public <T> T executeTransaction(@Nonnull TransactionalTask<T> task) throws TransactionException {
            return instance.executeTransaction(task);
        }

        @Override
        public <T> T executeTransaction(@Nonnull TransactionOptions options,
                                        @Nonnull TransactionalTask<T> task) throws TransactionException {
            return instance.executeTransaction(options, task);
        }

        @Override
        public TransactionContext newTransactionContext() {
            return instance.newTransactionContext();
        }

        @Override
        public TransactionContext newTransactionContext(@Nonnull TransactionOptions options) {
            return instance.newTransactionContext(options);
        }

        @Nonnull
        @Override
        public FlakeIdGenerator getFlakeIdGenerator(@Nonnull String name) {
            return instance.getFlakeIdGenerator(name);
        }

        @Nonnull
        @Override
        public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
            return instance.getReplicatedMap(name);
        }

        @Override
        public ICacheManager getCacheManager() {
            return instance.getCacheManager();
        }

        @Nonnull
        @Override
        public Cluster getCluster() {
            return instance.getCluster();
        }

        @Nonnull
        @Override
        public Endpoint getLocalEndpoint() {
            return instance.getLocalEndpoint();
        }

        @Override
        public Collection<DistributedObject> getDistributedObjects() {
            return instance.getDistributedObjects();
        }

        @Nonnull
        @Override
        public Config getConfig() {
            return instance.getConfig();
        }

        @Nonnull
        @Override
        public PartitionService getPartitionService() {
            return instance.getPartitionService();
        }

        @Nonnull
        @Override
        public SplitBrainProtectionService getSplitBrainProtectionService() {
            return instance.getSplitBrainProtectionService();
        }

        @Nonnull
        @Override
        public ClientService getClientService() {
            return instance.getClientService();
        }

        @Nonnull
        @Override
        public LoggingService getLoggingService() {
            return instance.getLoggingService();
        }

        @Nonnull
        @Override
        public LifecycleService getLifecycleService() {
            return instance.getLifecycleService();
        }

        @Nonnull
        @Override
        public <T extends DistributedObject> T getDistributedObject(@Nonnull String serviceName, @Nonnull String name) {
            return instance.getDistributedObject(serviceName, name);
        }

        @Override
        public UUID addDistributedObjectListener(@Nonnull DistributedObjectListener distributedObjectListener) {
            return instance.addDistributedObjectListener(distributedObjectListener);
        }

        @Override
        public boolean removeDistributedObjectListener(@Nonnull UUID registrationId) {
            return instance.removeDistributedObjectListener(registrationId);
        }

        @Nonnull
        @Override
        public ConcurrentMap<String, Object> getUserContext() {
            return instance.getUserContext();
        }

        @Nonnull
        @Override
        public HazelcastXAResource getXAResource() {
            return instance.getXAResource();
        }

        @Nonnull
        @Override
        public CardinalityEstimator getCardinalityEstimator(@Nonnull String name) {
            return instance.getCardinalityEstimator(name);
        }

        @Nonnull
        @Override
        public PNCounter getPNCounter(@Nonnull String name) {
            return instance.getPNCounter(name);
        }

        @Nonnull
        @Override
        public IScheduledExecutorService getScheduledExecutorService(@Nonnull String name) {
            return instance.getScheduledExecutorService(name);
        }

        @Nonnull
        @Override
        public CPSubsystem getCPSubsystem() {
            return instance.getCPSubsystem();
        }

        @Nonnull
        @Override
        public SqlService getSql() {
            return instance.getSql();
        }

        @Nonnull
        @Override
        public JetService getJet() {
            return jetProxy;
        }

        @Override
        public void shutdown() {
            getLifecycleService().shutdown();
        }
    }

    private static class BootstrappedJetProxy<M> extends AbstractJetInstance<M> {
        private final AbstractJetInstance<M> jet;
        private final String jar;
        private final String snapshotName;
        private final String jobName;
        private final Collection<Job> submittedJobs = new CopyOnWriteArrayList<>();

        BootstrappedJetProxy(
                @Nonnull JetService jet,
                @Nullable String jar,
                @Nullable String snapshotName,
                @Nullable String jobName
        ) {
            super(((AbstractJetInstance) jet).getHazelcastInstance());
            this.jet = (AbstractJetInstance<M>) jet;
            this.jar = jar;
            this.snapshotName = snapshotName;
            this.jobName = jobName;
        }

        @Nonnull @Override
        public String getName() {
            return jet.getName();
        }

        @Nonnull @Override
        public HazelcastInstance getHazelcastInstance() {
            return jet.getHazelcastInstance();
        }

        @Nonnull @Override
        public Cluster getCluster() {
            return jet.getCluster();
        }

        @Nonnull @Override
        public JetConfig getConfig() {
            return jet.getConfig();
        }

        @Nonnull @Override
        public Job newJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
            return remember(jet.newJob(pipeline, updateJobConfig(config)));
        }

        @Nonnull @Override
        public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
            return remember(jet.newJob(dag, updateJobConfig(config)));
        }

        @Nonnull @Override
        public Job newJobIfAbsent(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
            return remember(jet.newJobIfAbsent(pipeline, updateJobConfig(config)));
        }

        @Nonnull @Override
        public Job newJobIfAbsent(@Nonnull DAG dag, @Nonnull JobConfig config) {
            return remember(jet.newJobIfAbsent(dag, updateJobConfig(config)));
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
        public List<Job> getJobs(@Nonnull String name) {
            return jet.getJobs(name);
        }

        @Nonnull @Override
        public <K, V> IMap<K, V> getMap(@Nonnull String name) {
            return jet.getMap(name);
        }

        @Nonnull @Override
        public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
            return jet.getReplicatedMap(name);
        }

        @Nonnull @Override
        public JetCacheManager getCacheManager() {
            return jet.getCacheManager();
        }

        @Nonnull @Override
        public <E> IList<E> getList(@Nonnull String name) {
            return jet.getList(name);
        }

        @Nonnull @Override
        public <T> ITopic<T> getReliableTopic(@Nonnull String name) {
            return jet.getReliableTopic(name);
        }

        @Nonnull @Override
        public <T> Observable<T> getObservable(@Nonnull String name) {
            return jet.getObservable(name);
        }

        @Override
        public void shutdown() {
            jet.shutdown();
        }

        @Override
        public boolean existsDistributedObject(@Nonnull String serviceName, @Nonnull String objectName) {
            return jet.existsDistributedObject(serviceName, objectName);
        }

        @Override
        public ILogger getLogger() {
            return jet.getLogger();
        }

        @Override
        public Job newJobProxy(long jobId, M lightJobCoordinator) {
            return jet.newJobProxy(jobId, lightJobCoordinator);
        }

        @Override
        public Job newJobProxy(long jobId, boolean isLightJob, @Nonnull Object jobDefinition, @Nonnull JobConfig config) {
            return jet.newJobProxy(jobId, isLightJob, jobDefinition, config);
        }

        @Override
        public M getMasterId() {
            return jet.getMasterId();
        }

        @Override
        public Map<M, GetJobIdsResult> getJobsInt(String onlyName, Long onlyJobId) {
            return jet.getJobsInt(onlyName, onlyJobId);
        }
    }
}
