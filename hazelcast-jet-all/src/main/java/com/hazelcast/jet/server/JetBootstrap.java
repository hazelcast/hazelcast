/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetCacheManager;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.function.Supplier;
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
 *     Write your {@code main()} method and your Jet code the usual way, except
 *     for calling {@link JetBootstrap#getInstance()} to acquire a Jet client
 *     instance (instead of {@link Jet#newJetClient()}).
 * </li><li>
 *     Create a runnable JAR with your entry point declared as the {@code
 *     Main-Class} in {@code MANIFEST.MF}.
 * </li><li>
 *     Run your JAR, but instead of {@code java -jar jetjob.jar} use {@code
 *     jet-submit.sh jetjob.jar}. The script is found in the Jet distribution
 *     zipfile, in the {@code bin} directory. On Windows use {@code
 *     jet-submit.bat}.
 * </li><li>
 *     The Jet client will be configured from {@code hazelcast-client.xml}
 *     found in the {@code config} directory in Jet's distribution directory
 *     structure. Adjust that file to suit your needs.
 * </li></ol>
 *
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
 *
 * After building the JAR, submit the job:
 * <pre>
 * $ jet-submit.sh jetjob.jar
 * </pre>
 *
 */
public final class JetBootstrap {

    private static final Supplier<JetBootstrap> SUPPLIER =
            Util.memoizeConcurrent(() -> new JetBootstrap(Jet.newJetClient()));
    private static String jarPathname;
    private final JetInstance instance;

    private JetBootstrap(JetInstance instance) {
        this.instance = new InstanceProxy(instance);
    }

    /**
     * Runs the supplied JAR file and sets the static jar file name
     */
    public static void main(String[] args) throws Exception {
        int argLength = 1;
        if (args.length < argLength) {
            error(JetBootstrap.class.getSimpleName()
                    + ".main() must be called with the JAR filename as the first argument");
        }

        jarPathname = args[0];

        try (JarFile jarFile = new JarFile(jarPathname)) {
            if (jarFile.getManifest() == null) {
                error("No manifest file in " + jarPathname);
            }
            String mainClass = jarFile.getManifest().getMainAttributes().getValue("Main-Class");
            if (mainClass == null) {
                error("No Main-Class found in manifest");
            }
            String[] jobArgs = new String[args.length - argLength];
            System.arraycopy(args, argLength, jobArgs, 0, args.length - argLength);

            ClassLoader classLoader = JetBootstrap.class.getClassLoader();
            Class<?> clazz = classLoader.loadClass(mainClass);
            Method main = clazz.getDeclaredMethod("main", String[].class);
            int mods = main.getModifiers();
            if ((mods & Modifier.PUBLIC) == 0 || (mods & Modifier.STATIC) == 0) {
                error("Class " + clazz.getName()
                        + " has a main(String[] args) method which is not public static");
            }
            // upcast args to Object so it's passed as a single array-typed argument
            main.invoke(null, (Object) jobArgs);
        }
    }

    private static void error(String msg) {
        System.err.println(msg);
        System.exit(1);
    }

    /**
     * Returns the bootstrapped {@code JetInstance}.
     */
    public static JetInstance getInstance() {
        return SUPPLIER.get().instance;
    }

    private static class InstanceProxy implements JetInstance {

        private final JetInstance instance;

        InstanceProxy(JetInstance instance) {
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
            if (jarPathname != null) {
                config.addJar(jarPathname);
            }
            return instance.newJob(dag, config);
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
    }
}
