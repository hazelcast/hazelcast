/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JetBootstrap;

import javax.annotation.Nonnull;

/**
 * Entry point to the Jet product.
 *
 * @deprecated After 5.0 merge of Hazelcast products (IMDG and Jet into single
 * Hazelcast product), use the {@link Hazelcast} class as the entry point.
 */
public final class Jet {

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
     *     sure you use this method (instead of {@link HazelcastInstance#getJetInstance()})
     *     to acquire a Jet client instance.
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
     * @deprecated since 5.0
     * Please use {@link Hazelcast#bootstrappedInstance()} and then get
     * {@link JetInstance} from the created {@link HazelcastInstance}
     * by using {@link HazelcastInstance#getJetInstance()}.
     */
    @Nonnull
    @Deprecated
    public static JetInstance bootstrappedInstance() {
        return JetBootstrap.getInstance();
    }
}
