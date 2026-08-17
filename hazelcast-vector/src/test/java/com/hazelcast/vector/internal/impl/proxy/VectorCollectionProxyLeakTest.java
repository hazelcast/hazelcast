/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.proxy;

import com.google.common.collect.Lists;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.hazelcast.config.vector.VectorTestHelper.buildVectorCollectionConfig;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.randomVec;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.warmupCollection;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.warmupOneIndexCollection;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({NightlyTest.class, ParallelJVMTest.class})
public class VectorCollectionProxyLeakTest extends HazelcastTestSupport {
    private static final Logger LOGGER = LoggerFactory.getLogger(VectorCollectionProxyLeakTest.class);

    protected TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance[] members;
    private HazelcastInstance member;

    @Parameterized.Parameters(name = "clusterSize={0},deduplication={1}")
    public static Object[] parameters() {
        return Lists.cartesianProduct(List.of(List.of(1, 2), List.of(false, true))).stream()
                .map(tuple -> tuple.toArray(new Object[0]))
                .toArray(Object[]::new);
    }

    @Parameterized.Parameter
    public int clusterSize;

    @Parameterized.Parameter(1)
    public boolean useDeduplication;

    @Before
    public void setup() {
        members = factory.newInstances(getConfig(), clusterSize);
        member = members[0];
    }

    @After
    public void shutdownFactory() {
        factory.shutdownAll();
    }

    protected HazelcastInstance hz() {
        return member;
    }

    @Test(timeout = 600_000)
    public void testDestroyCollectionMemoryLeak() {
        // This test leaks mostly vector index support structures,
        // vector size and number of vectors is not so important:
        // minimal index (1 entry per partition) with small vector generates OOM the fastest.
        // It takes about 18k iterations to fill 4GB heap and 35k for 8GB with JVector 2.0.5.
        for (int i = 0; i < 100_000; ++i) {
            if (i % 1_000 == 0) {
                LOGGER.info("Destroy: {}", i);
            }
            var collectionName = "default_" + randomName();
            warmupOneIndexCollection(member, member.getVectorCollection(collectionName));
            hz().getVectorCollection(collectionName).destroy();
        }

        assertThat(hz().getDistributedObjects()).isEmpty();
    }

    @Test(timeout = 600_000)
    public void testCollectionEmptiedMemoryLeak() {
        // It takes about 200k iterations to fill 4GB heap and 400k for 8GB with JVector 2.0.5.
        var collectionName = randomName();
        VectorCollectionConfig vectorCollectionConfig = buildVectorCollectionConfig(collectionName, "default", 16, Metric.EUCLIDEAN);
        hz().getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        VectorCollection<Object, Object> collection = hz().getVectorCollection(vectorCollectionConfig.getName());
        VectorDocument<Object> document = VectorDocument.of("aa", randomVec(16));
        for (int i = 0; i < 1_000_000; ++i) {
            if (i % 10_000 == 0) {
                LOGGER.info("Empty: {}", i);
            }
            assertThat(collection.setAsync("1", document))
                    .succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
            assertThat(collection.deleteAsync("1"))
                    .succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
        }
    }

    @Test(timeout = 600_000)
    public void testCollectionEmptiedByUpdateMemoryLeak() {
        // It takes about 200k iterations to fill 4GB heap and 400k for 8GB with JVector 2.0.5.
        var collectionName = randomName();
        VectorCollectionConfig vectorCollectionConfig = buildVectorCollectionConfig(collectionName, "default", 16, Metric.EUCLIDEAN);
        hz().getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        VectorCollection<Object, Object> collection = hz().getVectorCollection(vectorCollectionConfig.getName());
        VectorDocument<Object> document = VectorDocument.of("aa", randomVec(16));
        assertThat(collection.setAsync("1", document))
                .succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
        for (int i = 0; i < 1_000_000; ++i) {
            if (i % 10_000 == 0) {
                LOGGER.info("Empty: {}", i);
            }
            assertThat(collection.setAsync("1", document))
                    .succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
        }
    }

    @Test(timeout = 600_000)
    public void testClearCollectionMemoryLeak() {
        var collectionName = randomName();
        for (int i = 0; i < 100_000; ++i) {
            if (i % 1_000 == 0) {
                LOGGER.info("Clear: {}", i);
            }
            warmupCollection(member, collectionName, 16, Metric.EUCLIDEAN);
            assertThat(hz().getVectorCollection(collectionName).clearAsync())
                    .succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
        }
    }

    @Override
    public Config getConfig() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        var vectorConfig = new VectorCollectionConfig();
        vectorConfig.setName("default_*");
        vectorConfig.addVectorIndexConfig(
                new VectorIndexConfig()
                        .setDimension(16)
                        .setMetric(Metric.EUCLIDEAN)
        );
        config.addVectorCollectionConfig(vectorConfig);
        return config;
    }
}
