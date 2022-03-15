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

package com.hazelcast.jet;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.PacketFiltersUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.idToString;
import static java.util.stream.Collectors.joining;

/**
 * Base class for tests that share the cluster for all jobs. The subclass must
 * call {@link #initialize} or {@link #initializeWithClient} method.
 */
@RunWith(HazelcastSerialClassRunner.class)
public abstract class SimpleTestInClusterSupport extends JetTestSupport {

    private static final ILogger SUPPORT_LOGGER = Logger.getLogger(SimpleTestInClusterSupport.class);

    private static TestHazelcastFactory factory;
    private static Config config;
    private static HazelcastInstance[] instances;
    private static HazelcastInstance client;

    protected static void initialize(int memberCount, @Nullable Config config) {
        assert factory == null : "already initialized";
        factory = new TestHazelcastFactory();
        instances = new HazelcastInstance[memberCount];
        if (config == null) {
            config = smallInstanceConfig();
        }
        SimpleTestInClusterSupport.config = config;
        // create members
        for (int i = 0; i < memberCount; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }
    }

    protected static void initializeWithClient(
            int memberCount,
            @Nullable Config config,
            @Nullable ClientConfig clientConfig
    ) {
        if (clientConfig == null) {
            clientConfig = new ClientConfig();
        }
        initialize(memberCount, config);
        client = factory.newHazelcastClient(clientConfig);
    }

    @After
    public void supportAfter() {
        if (instances == null) {
            return;
        }
        for (HazelcastInstance inst : instances) {
            PacketFiltersUtil.resetPacketFiltersFrom(inst);
        }
        // after each test ditch all jobs and objects
        List<Job> jobs = instances[0].getJet().getJobs();
        SUPPORT_LOGGER.info("Ditching " + jobs.size() + " jobs in SimpleTestInClusterSupport.@After: " +
                jobs.stream().map(j -> idToString(j.getId())).collect(joining(", ", "[", "]")));
        for (Job job : jobs) {
            ditchJob(job, instances());
        }
        // cancel all light jobs by cancelling their executions
        for (HazelcastInstance inst : instances) {
            JetServiceBackend jetServiceBackend = getJetServiceBackend(inst);
            jetServiceBackend.getJobExecutionService().cancelAllExecutions("ditching all jobs after a test");
            jetServiceBackend.getJobExecutionService().waitAllExecutionsTerminated();
        }
        Collection<DistributedObject> objects = instances()[0].getDistributedObjects();
        SUPPORT_LOGGER.info("Destroying " + objects.size()
                + " distributed objects in SimpleTestInClusterSupport.@After: "
                + objects.stream().map(o -> o.getServiceName() + "/" + o.getName())
                         .collect(Collectors.joining(", ", "[", "]")));
        for (DistributedObject o : objects) {
            o.destroy();
        }
    }

    @AfterClass
    public static void supportAfterClass() throws Exception {
        try {
            if (factory != null) {
                SUPPORT_LOGGER.info("Terminating instance factory in SimpleTestInClusterSupport.@AfterClass");
                spawn(() -> factory.terminateAll())
                        .get(1, TimeUnit.MINUTES);
            }
        } catch (Exception e) {
            // Log the exception, so it is visible in log file for the test class,
            // otherwise it is only visible in surefire test report
            SUPPORT_LOGGER.warning("Terminating instance factory failed", e);
            throw e;
        } finally {
            factory = null;
            instances = null;
            client = null;
        }
    }

    @Nonnull
    protected static TestHazelcastFactory factory() {
        return factory;
    }

    /**
     * Returns the config used to create member instances (even if null was
     * passed).
     */
    @Nonnull
    protected static Config jetConfig() {
        return config;
    }

    /**
     * Returns the first instance.
     */
    @Nonnull
    protected static HazelcastInstance instance() {
        return instances[0];
    }

    /**
     * Returns all instances (except for the client).
     */
    @Nonnull
    protected static HazelcastInstance[] instances() {
        return instances;
    }

    /**
     * Returns the client or null, if a client wasn't requested.
     */
    protected static HazelcastInstance client() {
        return client;
    }
}
