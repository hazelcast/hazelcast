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

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.spi.properties.ClusterProperty;
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

    protected static JetTestInstanceFactory factory = new JetTestInstanceFactory();
    private static JetInstance[] allJetInstances;

    protected static JetInstance member;
    protected static JetInstance client;

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
        client = factory.newClient();
    }

    protected static Config prepareConfig() {
        parallelism = Runtime.getRuntime().availableProcessors() / MEMBER_COUNT / 2;
        Config config = smallInstanceConfig();

        JetConfig jetConfig = config.getJetConfig();
        jetConfig.getInstanceConfig().setCooperativeThreadCount(max(2, parallelism));
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
        // Set partition count to match the parallelism of IMap sources.
        // Their preferred local parallelism is 2, therefore partition count
        // should be 2 * MEMBER_COUNT.
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "" + 2 * MEMBER_COUNT);
        config.addCacheConfig(new CacheSimpleConfig().setName("*"));
        config.getMapConfig(JOURNALED_MAP_PREFIX + '*').getEventJournalConfig().setEnabled(true);
        config.getCacheConfig(JOURNALED_CACHE_PREFIX + '*').getEventJournalConfig().setEnabled(true);
        return config;
    }

    @AfterClass
    public static void tearDown() throws Exception {
        spawn(() -> factory.terminateAll())
                .get(1, TimeUnit.MINUTES);

        factory = null;
        allJetInstances = null;
        member = null;
        client = null;
    }

    @After
    public void after() throws Exception {
        Future future = spawn(() ->
                cleanUpCluster(allJetInstances()));
        future.get(1, TimeUnit.MINUTES);
    }

    protected JetInstance jet() {
        return testMode.getJet();
    }

    protected Job execute(Pipeline p, JobConfig config) {
        Job job = jet().newJob(p, config);
        job.join();
        return job;
    }

    protected static JetInstance[] allJetInstances() {
        return allJetInstances;
    }

    private static JetInstance createCluster(int nodeCount, Config config) {
        factory = new JetTestInstanceFactory();
        allJetInstances = new JetInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            allJetInstances[i] = factory.newMember(config);
        }
        return allJetInstances[0];
    }

    protected static final class TestMode {

        private final String name;
        private final Supplier<JetInstance> getJetFn;

        TestMode(String name, Supplier<JetInstance> getJetFn) {
            this.name = name;
            this.getJetFn = getJetFn;
        }

        public JetInstance getJet() {
            return getJetFn.get();
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
