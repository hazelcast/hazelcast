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

package com.hazelcast.jet;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.function.Supplier;

@RunWith(Parameterized.class)
@Category(ParallelTest.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public abstract class TestInClusterSupport extends JetTestSupport {

    public static final String JOURNALED_MAP_PREFIX = "journaledMap.";
    public static final String JOURNALED_CACHE_PREFIX = "journaledCache.";

    private static final int MEMBER_COUNT = 2;

    private static JetTestInstanceFactory factory = new JetTestInstanceFactory();
    private static JetInstance[] allJetInstances;

    private static JetInstance member;
    private static JetInstance client;

    private static final TestMode MEMBER_TEST_MODE = new TestMode("member", () -> member);
    private static final TestMode CLIENT_TEST_MODE = new TestMode("client", () -> client);

    @Parameter
    public TestMode testMode;

    @Parameters(name = "{index}: mode={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(MEMBER_TEST_MODE, CLIENT_TEST_MODE);
    }

    @BeforeClass
    public static void setupCluster() {
        int parallelism = Runtime.getRuntime().availableProcessors() / MEMBER_COUNT / 2;
        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(parallelism <= 2 ? 2 : parallelism);
        Config hzConfig = config.getHazelcastConfig();
        hzConfig.addCacheConfig(new CacheSimpleConfig().setName("*"));
        hzConfig.getMapEventJournalConfig(JOURNALED_MAP_PREFIX + '*').setEnabled(true);
        hzConfig.getCacheEventJournalConfig(JOURNALED_CACHE_PREFIX + '*').setEnabled(true);
        member = createCluster(MEMBER_COUNT, config);
        client = factory.newClient();
    }

    @AfterClass
    public static void tearDown() {
        factory.terminateAll();
        factory = null;
        allJetInstances = null;
        member = null;
        client = null;
    }

    protected static JetInstance[] allJetInstances() {
        return allJetInstances;
    }

    private static JetInstance createCluster(int nodeCount, JetConfig config) {
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
