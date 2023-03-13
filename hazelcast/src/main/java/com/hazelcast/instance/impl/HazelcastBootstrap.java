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
import com.hazelcast.jet.JetCacheManager;
import com.hazelcast.jet.JetException;
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
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
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

    public static void resetSupplier() {
        supplier = null;
    }

    public static synchronized void executeJar(@Nonnull Supplier<HazelcastInstance> supplierOfInstance,
                                               @Nonnull String jarPath,
                                               @Nullable String snapshotName,
                                               @Nullable String jobName,
                                               @Nullable String mainClassName,
                                               @Nonnull List<String> args,
                                               boolean calledByMember
    ) throws Exception {

        // Method is synchronized, so it is safe to do null check
        if (supplier == null) {
            supplier = new ConcurrentMemoizingSupplier<>(() ->
                    BootstrappedInstanceProxy.createWithJetProxy(supplierOfInstance.get(), jarPath, snapshotName, jobName));
        }

        if (calledByMember) {
            resetJetParametersForMember(jarPath, snapshotName, jobName);
        }

        try {
            mainClassName = findMainClassNameForJar(mainClassName, jarPath, calledByMember);

            URL jarUrl = new File(jarPath).toURI().toURL();
            try (URLClassLoader classLoader = URLClassLoader.newInstance(
                    new URL[]{jarUrl},
                    HazelcastBootstrap.class.getClassLoader())) {

                Method main = findMainMethodForJar(classLoader, mainClassName, calledByMember);

                String[] jobArgs = args.toArray(new String[0]);
                // upcast args to Object so it's passed as a single array-typed argument
                main.invoke(null, (Object) jobArgs);
            }

            // Wait for the job to start only if called by the client side
            if (!calledByMember) {
                awaitJobsStarted();
            }

        } finally {
            // HazelcastInstance should be closed only if called by the client side
            if (!calledByMember) {
                HazelcastInstance remembered = HazelcastBootstrap.supplier.remembered();
                if (remembered != null) {
                    try {
                        remembered.shutdown();
                    } catch (Exception exception) {
                        LOGGER.severe("Shutdown failed with:", exception);
                    }
                }
                resetSupplier();
            }
        }
    }

    static String findMainClassNameForJar(String mainClass, String jarPath, boolean calledByMember)
            throws IOException {
        MainClassNameFinder mainClassNameFinder = new MainClassNameFinder();
        mainClassNameFinder.findMainClass(mainClass, jarPath);

        if (mainClassNameFinder.hasError()) {
            String errorMessage = mainClassNameFinder.getErrorMessage();
            // Client side immediately exits, server side throws JetException
            error(errorMessage, calledByMember);
        }
        return mainClassNameFinder.getMainClassName();
    }

    static Method findMainMethodForJar(ClassLoader classLoader, String mainClassName, boolean calledByMember)
            throws ClassNotFoundException {
        MainMethodFinder mainMethodFinder = new MainMethodFinder();
        MainMethodFinder.Result result = mainMethodFinder.findMainMethod(classLoader, mainClassName);

        // Check if there is an error

        if (result.hasError()) {
            // Client side immediately exits, server side throws JetException
            String errorMessage = result.getErrorMessage();
            error(errorMessage, calledByMember);
        }
        return result.getMainMethod();
    }

    private static void resetJetParametersForMember(String jar, String snapshotName, String jobName) {
        // BootstrappedInstanceProxy is a singleton that owns a HazelcastInstance and BootstrappedJetProxy
        // Change the state of the singleton
        BootstrappedInstanceProxy bootstrappedInstanceProxy = supplier.get();

        BootstrappedJetProxy bootstrappedJetProxy = bootstrappedInstanceProxy.getJet();
        // Clear all previously submitted jobs to avoid memory leak
        bootstrappedJetProxy.clearSubmittedJobs();
        bootstrappedJetProxy.setJarName(jar);
        bootstrappedJetProxy.setSnapshotName(snapshotName);
        bootstrappedJetProxy.setJobName(jobName);
    }


    // Method is call by synchronized executeJar() so, it is safe to access submittedJobs array in BootstrappedJetProxy
    private static void awaitJobsStarted() {

        List<Job> submittedJobs = (HazelcastBootstrap.supplier.get().getJet()).submittedJobs();
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
                                       + toLocalDateTime(job.getSubmissionTime()) + " changed status to "
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

    private static void error(String msg, boolean calledByMember) {
        if (!calledByMember) {
            System.err.println(msg);
            System.exit(1);
        } else {
            throw new JetException(msg);
        }
    }

    /**
     * Returns the bootstrapped {@code HazelcastInstance}. The instance will be
     * automatically shut down once the {@code main()} method of the JAR returns.
     */
    @Nonnull
    public static synchronized HazelcastInstance getInstance() {
        if (supplier == null) {
            supplier = new ConcurrentMemoizingSupplier<>(() ->
                    BootstrappedInstanceProxy.createWithoutJetProxy(createStandaloneInstance()));
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

    // A special HazelcastInstance that has a BootstrappedJetProxy
    @SuppressWarnings({"checkstyle:methodcount"})
    private static class BootstrappedInstanceProxy implements HazelcastInstance {
        private final HazelcastInstance instance;
        private final BootstrappedJetProxy jetProxy;

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

        static BootstrappedInstanceProxy createWithoutJetProxy(@Nonnull HazelcastInstance instance) {
            return new BootstrappedInstanceProxy(instance);
        }

        static BootstrappedInstanceProxy createWithJetProxy(
                @Nonnull HazelcastInstance instance,
                @Nullable String jar,
                @Nullable String snapshotName,
                @Nullable String jobName
        ) {
            return new BootstrappedInstanceProxy(instance, jar, snapshotName, jobName);
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
        public BootstrappedJetProxy getJet() {
            return jetProxy;
        }

        @Override
        public void shutdown() {
            getLifecycleService().shutdown();
        }
    }

    private static class BootstrappedJetProxy<M> extends AbstractJetInstance<M> {
        private final AbstractJetInstance<M> jet;
        private String jar;
        private String snapshotName;
        private String jobName;
        private final CopyOnWriteArrayList<Job> submittedJobs = new CopyOnWriteArrayList<>();

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

        @Nonnull
        @Override
        public String getName() {
            return jet.getName();
        }

        @Nonnull
        @Override
        public HazelcastInstance getHazelcastInstance() {
            return jet.getHazelcastInstance();
        }

        @Nonnull
        @Override
        public Cluster getCluster() {
            return jet.getCluster();
        }

        @Nonnull
        @Override
        public JetConfig getConfig() {
            return jet.getConfig();
        }

        @Nonnull
        @Override
        public Job newJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
            return addToSubmittedJobs(jet.newJob(pipeline, updateJobConfig(config)));
        }

        @Nonnull
        @Override
        public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
            return addToSubmittedJobs(jet.newJob(dag, updateJobConfig(config)));
        }

        @Nonnull
        @Override
        public Job newJobIfAbsent(@Nonnull Pipeline pipeline, @Nonnull JobConfig config) {
            return addToSubmittedJobs(jet.newJobIfAbsent(pipeline, updateJobConfig(config)));
        }

        @Nonnull
        @Override
        public Job newJobIfAbsent(@Nonnull DAG dag, @Nonnull JobConfig config) {
            return addToSubmittedJobs(jet.newJobIfAbsent(dag, updateJobConfig(config)));
        }

        void clearSubmittedJobs() {
            submittedJobs.clear();
        }

        List<Job> submittedJobs() {
            return new ArrayList<>(submittedJobs);
        }

        private Job addToSubmittedJobs(@Nonnull Job job) {
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

        @Nonnull
        @Override
        public List<Job> getJobs(@Nonnull String name) {
            return jet.getJobs(name);
        }

        @Nonnull
        @Override
        public <K, V> IMap<K, V> getMap(@Nonnull String name) {
            return jet.getMap(name);
        }

        @Nonnull
        @Override
        public <K, V> ReplicatedMap<K, V> getReplicatedMap(@Nonnull String name) {
            return jet.getReplicatedMap(name);
        }

        @Nonnull
        @Override
        public JetCacheManager getCacheManager() {
            return jet.getCacheManager();
        }

        @Nonnull
        @Override
        public <E> IList<E> getList(@Nonnull String name) {
            return jet.getList(name);
        }

        @Nonnull
        @Override
        public <T> ITopic<T> getReliableTopic(@Nonnull String name) {
            return jet.getReliableTopic(name);
        }

        @Nonnull
        @Override
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

        public void setJarName(String jar) {
            this.jar = jar;
        }

        public void setSnapshotName(String snapshotName) {
            this.snapshotName = snapshotName;
        }

        public void setJobName(String jobName) {
            this.jobName = jobName;
        }
    }
}
