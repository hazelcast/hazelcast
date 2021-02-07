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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.lang.Math.max;

/**
 * Extends {@link JetTestSupport} in such a way that one cluster is used for
 * all tests in the class.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@SuppressWarnings("checkstyle:declarationorder")
public abstract class TestInClusterSupport extends JetTestSupport {

    protected static final String JOURNALED_MAP_PREFIX = "journaledMap.";
    protected static final String JOURNALED_CACHE_PREFIX = "journaledCache.";
    protected static final int MEMBER_COUNT = 2;

    protected static TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private static HazelcastInstance[] instances;

    protected static HazelcastInstance member;
    protected static HazelcastInstance client;

    private static final TestMode MEMBER_TEST_MODE = new TestMode("member", () -> member);
    private static final TestMode CLIENT_TEST_MODE = new TestMode("client", () -> client);
    protected static int parallelism;

    @Parameter
    public TestMode testMode;

    @Parameters(name = "{index}: mode={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(MEMBER_TEST_MODE, CLIENT_TEST_MODE);
    }

    @BeforeClass
    public static void setupCluster() {
        member = createCluster(MEMBER_COUNT, prepareConfig());
        client = hazelcastFactory.newHazelcastClient();
    }

    protected static Config prepareConfig() {
        Config config = new Config();
        parallelism = Runtime.getRuntime().availableProcessors() / MEMBER_COUNT / 2;
        JetConfig jetConfig = new JetConfig();
        jetConfig.getInstanceConfig().setCooperativeThreadCount(max(2, parallelism));
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
        // Set partition count to match the parallelism of IMap sources.
        // Their preferred local parallelism is 2, therefore partition count
        // should be 2 * MEMBER_COUNT.
        config.getProperties().setProperty("hazelcast.partition.count", "" + 2 * MEMBER_COUNT);
        config.addCacheConfig(new CacheSimpleConfig().setName("*"));
        config.getMapConfig(JOURNALED_MAP_PREFIX + '*').getEventJournalConfig().setEnabled(true);
        config.getCacheConfig(JOURNALED_CACHE_PREFIX + '*').getEventJournalConfig().setEnabled(true);
        return config;
    }

    @AfterClass
    public static void tearDown() throws Exception {
        spawn(() -> hazelcastFactory.terminateAll())
                .get(1, TimeUnit.MINUTES);

        hazelcastFactory = null;
        instances = null;
        member = null;
        client = null;
    }

    @After
    public void after() throws Exception {
        Future future = spawn(() ->
                cleanUpCluster(allInstances()));
        future.get(1, TimeUnit.MINUTES);
    }

    protected JetInstance jet() {
        return testMode.getInstance().getJetInstance();
    }

    protected HazelcastInstance instance() {
        return testMode.getInstance();
    }

    protected Job execute(Pipeline p, JobConfig config) {
        Job job = jet().newJob(p, config);
        job.join();
        return job;
    }

    protected static HazelcastInstance[] allInstances() {
        return instances;
    }

    private static HazelcastInstance createCluster(int nodeCount, Config config) {
        hazelcastFactory = new TestHazelcastFactory();
        instances = new HazelcastInstance[nodeCount];
        instances = hazelcastFactory.newInstances(config, nodeCount);
        return instances[0];
    }

    protected static final class TestMode {

        private final String name;
        private final Supplier<HazelcastInstance> getJetFn;

        TestMode(String name, Supplier<HazelcastInstance> getJetFn) {
            this.name = name;
            this.getJetFn = getJetFn;
        }

        public HazelcastInstance getInstance() {
            return getJetFn.get();
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
