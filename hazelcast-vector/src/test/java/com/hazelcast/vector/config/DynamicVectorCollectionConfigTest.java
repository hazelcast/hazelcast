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

package com.hazelcast.vector.config;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.config.vector.VectorTestHelper.buildVectorCollectionConfig;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicVectorCollectionConfigTest extends HazelcastTestSupport {

    protected TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    HazelcastInstance instance;

    @Before
    public void setup() {
        var config = smallInstanceConfig();
        config.getSplitBrainProtectionConfig("atLeast3Members").setEnabled(true).setMinimumClusterSize(3);
        instance = hazelcastFactory.newHazelcastInstance(config);
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Test
    public void clientTest_addAndGetVectorCollectionConfig_then_success() {
        var vectorCollectionConfig1 = buildVectorCollectionConfig(
                "vector_collection-1",
                "index-1-1",
                3,
                Metric.COSINE,
                10,
                10,
                true
        );
        vectorCollectionConfig1.addVectorIndexConfig(new VectorIndexConfig().setName("index-1-2").setDimension(5).setMetric(Metric.EUCLIDEAN));
        var vectorCollectionConfig2 = buildVectorCollectionConfig(
                "vector_collection-2",
                "index-2-1",
                2,
                Metric.DOT
        ).setBackupCount(2)
                .setAsyncBackupCount(1)
                .setUserCodeNamespace("ns1");

        var client = hazelcastFactory.newHazelcastClient();
        client.getConfig().addVectorCollectionConfig(vectorCollectionConfig1);
        client.getConfig().addVectorCollectionConfig(vectorCollectionConfig2);

        var actual = instance.getConfig().getVectorCollectionConfigs();
        assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(
                        Map.of(
                                "vector_collection-1", vectorCollectionConfig1,
                                "vector_collection-2", vectorCollectionConfig2
                        )
                );
    }


    @Test
    public void clientTest_addAndGetVectorCollectionConfigWithIndexWithoutName_then_success() {
        var vectorCollectionConfig = new VectorCollectionConfig("vector_collection-1")
                .addVectorIndexConfig(
                        new VectorIndexConfig().setDimension(3).setMetric(Metric.COSINE)
                );

        var client = hazelcastFactory.newHazelcastClient();
        client.getConfig().addVectorCollectionConfig(vectorCollectionConfig);

        var actual = instance.getConfig().getVectorCollectionConfigs();
        assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(
                        Map.of(
                                "vector_collection-1", vectorCollectionConfig
                        )
                );
    }

    @Test
    public void clientTest_addAndGetVectorCollectionConfigWithSplitBrainConfiguration_then_success() {
        var vectorCollectionConfig = new VectorCollectionConfig("vector_collection-1")
                .addVectorIndexConfig(
                        new VectorIndexConfig().setDimension(3).setMetric(Metric.COSINE)
                )
                .setSplitBrainProtectionName("atLeast3Members")
                .setMergePolicyConfig(new MergePolicyConfig("PutIfAbsentMergePolicy", 132));

        var client = hazelcastFactory.newHazelcastClient();
        client.getConfig().addVectorCollectionConfig(vectorCollectionConfig);

        var actual = instance.getConfig().getVectorCollectionConfigs();
        assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(
                        Map.of(
                                "vector_collection-1", vectorCollectionConfig
                        )
                );
    }

    @Test
    public void clientTest_addBackupCountOneConfig_then_success() {
        clientTest_addBackupConfig("vector-collection-1", 1, null);
        var actual = instance.getConfig().getVectorCollectionConfigOrNull("vector-collection-1");
        assertThat(actual.getBackupCount()).isEqualTo(1);
    }

    @Test
    public void clientTest_addBackupCountMaxConfig_then_success() {
        clientTest_addBackupConfig("vector-collection-1", 6, null);
        var actual = instance.getConfig().getVectorCollectionConfigOrNull("vector-collection-1");
        assertThat(actual.getBackupCount()).isEqualTo(6);
    }

    @Test
    public void clientTest_addBackupCountMinConfig_then_success() {
        clientTest_addBackupConfig("vector-collection-1", 0, null);
        var actual = instance.getConfig().getVectorCollectionConfigOrNull("vector-collection-1");
        assertThat(actual.getBackupCount()).isEqualTo(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void clientTest_addBackupCountMoreThanMaxConfig_then_fail() {
        clientTest_addBackupConfig("vector-collection-1", 7, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void clientTest_addBackupCountLessThanMinConfig_then_fail() {
        clientTest_addBackupConfig("vector-collection-1", -1, null);
    }

    @Test
    public void clientTest_addAsyncBackupCountOneConfig_then_success() {
        clientTest_addBackupConfig("vector-collection-1", null, 1);
        var actual = instance.getConfig().getVectorCollectionConfigOrNull("vector-collection-1");
        assertThat(actual.getAsyncBackupCount()).isEqualTo(1);
    }

    @Test
    public void clientTest_addAsyncBackupCountMaxConfig_then_success() {
        clientTest_addBackupConfig("vector-collection-1", 0, 6);
        var actual = instance.getConfig().getVectorCollectionConfigOrNull("vector-collection-1");
        assertThat(actual.getAsyncBackupCount()).isEqualTo(6);
    }

    @Test
    public void clientTest_addAsyncBackupCountMinConfig_then_success() {
        clientTest_addBackupConfig("vector-collection-1", null, 0);
        var actual = instance.getConfig().getVectorCollectionConfigOrNull("vector-collection-1");
        assertThat(actual.getAsyncBackupCount()).isEqualTo(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void clientTest_addAsyncBackupCountMoreThanMaxConfig_then_fail() {
        clientTest_addBackupConfig("vector-collection-1", 0, 7);
    }

    @Test(expected = IllegalArgumentException.class)
    public void clientTest_addAsyncBackupCountLessThanMinConfig_then_fail() {
        clientTest_addBackupConfig("vector-collection-1", null, -1);
    }

    @Test
    public void clientTest_addSyncAndAsyncBackupCountMaxConfig_then_success() {
        clientTest_addBackupConfig("vector-collection-1", 4, 2);
        var actual = instance.getConfig().getVectorCollectionConfigOrNull("vector-collection-1");
        assertThat(actual.getBackupCount()).isEqualTo(4);
        assertThat(actual.getAsyncBackupCount()).isEqualTo(2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void clientTest_addSyncAndAsyncBackupCountMoreThanMaxConfig_then_fail() {
        clientTest_addBackupConfig("vector-collection-1", 4, 3);
    }

    private void clientTest_addBackupConfig(String name, Integer backupCount, Integer asyncBackupCount) {
        var vectorCollectionConfig
                = buildVectorCollectionForBackupTests(name, backupCount, asyncBackupCount);
        var client = hazelcastFactory.newHazelcastClient();
        client.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
    }

    private VectorCollectionConfig buildVectorCollectionForBackupTests(
            String name, Integer backupCount, Integer asyncBackupCount) {
        return buildVectorCollectionConfig(
                name,
                "index-1",
                1,
                Metric.COSINE,
                11,
                12,
                true,
                backupCount,
                asyncBackupCount
        );
    }
}
