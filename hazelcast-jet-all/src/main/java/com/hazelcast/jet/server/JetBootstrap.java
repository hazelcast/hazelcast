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

package com.hazelcast.jet.server;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetCacheManager;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.AbstractJetInstance;
import com.hazelcast.jet.impl.util.ConcurrentMemoizingSupplier;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.jar.JarFile;

/**
 * A helper class that allows one to create a standalone runnable JAR which
 * contains all the code needed to submit a job to a running Jet cluster.
 * The main issue with achieving this is that the JAR must be attached as a
 * resource to the job being submitted, so the Jet cluster will be able to
 * load and use its classes. However, from within a running {@code main()}
 * method it is not trivial to find out the filename of the JAR containing
 * it.
 * <p>
 * This helper is a part of the solution to the above "bootstrapping"
 * issue. To use it, follow these steps:
 * <ol><li>
 * Write your {@code main()} method and your Jet code the usual way, except
 * for calling {@link JetBootstrap#getInstance()} to acquire a Jet client
 * instance (instead of {@link Jet#newJetClient()}).
 * </li><li>
 * Create a runnable JAR with your entry point declared as the {@code
 * Main-Class} in {@code MANIFEST.MF}.
 * </li><li>
 * Run your JAR, but instead of {@code java -jar jetjob.jar} use {@code
 * jet.sh submit jetjob.jar}. The script is found in the Jet distribution
 * zipfile, in the {@code bin} directory. On Windows use {@code
 * jet.bat}.
 * </li><li>
 * The Jet client will be configured from {@code hazelcast-client.xml}
 * found in the {@code config} directory in Jet's distribution directory
 * structure. Adjust that file to suit your needs.
 * </li></ol>
 * <p>
 * For example, write a class like this:
 * <pre>
 * public class CustomJetJob {
 *   public static void main(String[] args) {
 *     JetInstance jet = JetBootstrap.getInstance();
 *     jet.newJob(buildDag()).execute().get();
 *   }
 *
 *   public static DAG buildDag() {
 *       // ...
 *   }
 * }
 * </pre>
 * <p>
 * After building the JAR, submit the job:
 * <pre>
 * $ jet.sh submit jetjob.jar
 * </pre>
 */
public final class JetBootstrap {

    // these params must be set before a job is submitted
    private static ClientConfig config;
    private static String jarName;
    private static String snapshotName;
    private static String jobName;

    private static final ConcurrentMemoizingSupplier<JetBootstrap> SUPPLIER =
            new ConcurrentMemoizingSupplier<>(() -> new JetBootstrap(Jet.newJetClient(config)));

    private final JetInstance instance;

    private JetBootstrap(JetInstance instance) {
        this.instance = new InstanceProxy((AbstractJetInstance) instance);
    }

    static void executeJar(
            @Nonnull ClientConfig clientConfig, @Nonnull String jar, @Nullable String snapshotName,
            @Nullable String jobName, @Nonnull List<String> args
    ) throws Exception {
        JetBootstrap.config = clientConfig;
        JetBootstrap.jarName = jar;
        JetBootstrap.snapshotName = snapshotName;
        JetBootstrap.jobName = jobName;

        try (JarFile jarFile = new JarFile(jar)) {
            if (jarFile.getManifest() == null) {
                error("No manifest file in " + jar);
            }
            String mainClass = jarFile.getManifest().getMainAttributes().getValue("Main-Class");
            if (mainClass == null) {
                error("No Main-Class found in manifest");
            }

            URL jarUrl = new URL("file:///" + jar);
            URLClassLoader classLoader = AccessController.doPrivileged(
                    (PrivilegedAction<URLClassLoader>) () ->
                            new URLClassLoader(new URL[]{jarUrl}, JetBootstrap.class.getClassLoader())
            );

            Class<?> clazz = classLoader.loadClass(mainClass);
            Method main = clazz.getDeclaredMethod("main", String[].class);
            int mods = main.getModifiers();
            if ((mods & Modifier.PUBLIC) == 0 || (mods & Modifier.STATIC) == 0) {
                error("Class " + clazz.getName()
                        + " has a main(String[] args) method which is not public static");
            }
            String[] jobArgs = args.toArray(new String[0]);
            // upcast args to Object so it's passed as a single array-typed argument
            main.invoke(null, (Object) jobArgs);
        } finally {
            JetBootstrap remembered = SUPPLIER.remembered();
            if (remembered != null) {
                remembered.instance.shutdown();
            }
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
    public static JetInstance getInstance() {
        return SUPPLIER.get().instance;
    }

    private static class InstanceProxy extends AbstractJetInstance {

        private final AbstractJetInstance instance;

        InstanceProxy(AbstractJetInstance instance) {
            super(instance.getHazelcastInstance());
            this.instance = instance;
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
        public Job newJob(@Nonnull DAG dag) {
            return newJob(dag, new JobConfig());
        }

        @Nonnull @Override
        public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
            return instance.newJob(dag, updateJobConfig(config));
        }

        @Nonnull @Override
        public Job newJobIfAbsent(@Nonnull DAG dag, @Nonnull JobConfig config) {
            return instance.newJobIfAbsent(dag, updateJobConfig(config));
        }

        private JobConfig updateJobConfig(@Nonnull JobConfig config) {
            if (jarName != null) {
                config.addJar(jarName);
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
        public <K, V> IMapJet<K, V> getMap(@Nonnull String name) {
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
        public <E> IListJet<E> getList(@Nonnull String name) {
            return instance.getList(name);
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
        public Job newJobProxy(long jobId, DAG dag, JobConfig config) {
            return instance.newJobProxy(jobId, dag, config);
        }

        @Override
        public List<Long> getJobIdsByName(String name) {
            return instance.getJobIdsByName(name);
        }
    }
}
