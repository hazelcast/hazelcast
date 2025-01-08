/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.vector;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.InvalidConfigurationException;
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

import java.util.Collections;
import java.util.Map;

import static com.hazelcast.config.vector.VectorTestHelper.buildVectorCollectionConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicVectorCollectionConfigTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    HazelcastInstance instance1;
    HazelcastInstance instance2;

    HazelcastInstance client;

    @Before
    public void setup() {
        instance1 = hazelcastFactory.newHazelcastInstance(smallInstanceConfigWithoutJetAndMetrics());
        instance2 = hazelcastFactory.newHazelcastInstance(smallInstanceConfigWithoutJetAndMetrics());
        client = hazelcastFactory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Test
    public void memberTest_addAndGetOneVectorCollection_then_success() {
        String vectorCollection = "vector-collection-1";
        var vectorCollectionConfig = buildVectorCollectionConfig(
                vectorCollection,
                "index-1",
                1,
                Metric.COSINE,
                11,
                12,
                true
        );
        instance1.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        var actual = instance2.getConfig().getVectorCollectionConfigOrNull(vectorCollection);
        assertThat(actual).isEqualTo(vectorCollectionConfig);
    }

    @Test
    public void memberTest_addAndGetVectorCollectionWithoutIndexName_then_success() {
        String vectorCollection = "vector-collection-1";
        var vectorCollectionConfig = new VectorCollectionConfig(vectorCollection)
                .addVectorIndexConfig(new VectorIndexConfig().setMetric(Metric.EUCLIDEAN).setDimension(2));
        instance1.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        var actual = instance2.getConfig().getVectorCollectionConfigOrNull(vectorCollection);
        assertThat(actual).isEqualTo(vectorCollectionConfig);
    }

    @Test
    public void memberTest_addAndGetSeveralVectorCollection_then_success() {
        var vectorCollectionConfig1 = buildVectorCollectionConfig("vector-1", "index-1", 1, Metric.COSINE);
        vectorCollectionConfig1.setBackupCount(2);
        vectorCollectionConfig1.setAsyncBackupCount(1);
        vectorCollectionConfig1.addVectorIndexConfig(
                new VectorIndexConfig().setMetric(Metric.DOT).setName("index-2").setDimension(1).setMaxDegree(11).setEfConstruction(12).setUseDeduplication(true)
        );
        var vectorCollectionConfig2 = buildVectorCollectionConfig("vector-2", "index-1", 1, Metric.COSINE);
        vectorCollectionConfig2.setBackupCount(3);
        vectorCollectionConfig2.setAsyncBackupCount(2);

        instance1.getConfig().addVectorCollectionConfig(vectorCollectionConfig1);
        instance1.getConfig().addVectorCollectionConfig(vectorCollectionConfig2);

        var actual = instance2.getConfig().getVectorCollectionConfigs();
        assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(Map.of(
                        vectorCollectionConfig1.getName(), vectorCollectionConfig1,
                        vectorCollectionConfig2.getName(), vectorCollectionConfig2
                ));
    }

    @Test
    public void memberTest_getConfigOnNewMember_then_success() {
        String vectorCollection = "vector-collection-1";
        var vectorCollectionConfig = buildVectorCollectionConfig(
                vectorCollection,
                "index-1",
                1,
                Metric.COSINE,
                11,
                12,
                true
        );
        instance1.getConfig().addVectorCollectionConfig(vectorCollectionConfig);

        var instance3 = hazelcastFactory.newHazelcastInstance(smallInstanceConfigWithoutJetAndMetrics());
        waitAllForSafeState(instance1, instance2, instance3);

        var actual = instance3.getConfig().getVectorCollectionConfigOrNull(vectorCollection);
        assertThat(actual).isEqualTo(vectorCollectionConfig);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void memberTest_addConfigWithTheSameName_then_fail() {
        var vectorCollectionConfig1 = buildVectorCollectionConfig("vector-1", "index-1", 1, Metric.COSINE);
        var vectorCollectionConfig2 = buildVectorCollectionConfig("vector-1", "index-1", 2, Metric.COSINE);

        instance1.getConfig().addVectorCollectionConfig(vectorCollectionConfig1);
        instance1.getConfig().addVectorCollectionConfig(vectorCollectionConfig2);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void memberTest_addConfigWithTheSameNameDifferentBackupCount_then_fail() {
        var vectorCollectionConfig = buildVectorCollectionConfig("vector-1", "index-1", 1, Metric.COSINE);

        instance1.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        instance1.getConfig().addVectorCollectionConfig(vectorCollectionConfig.setBackupCount(2));
    }

    @Test(expected = InvalidConfigurationException.class)
    public void memberTest_addConfigWithTheSameNameDifferentAsyncBackupCount_then_fail() {
        var vectorCollectionConfig = buildVectorCollectionConfig("vector-1", "index-1", 1, Metric.COSINE);

        instance1.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        instance1.getConfig().addVectorCollectionConfig(vectorCollectionConfig.setAsyncBackupCount(2));
    }

    @Test
    public void memberTest_addTheSameConfigTwice_then_success() {
        var vectorCollectionConfig = buildVectorCollectionConfig("vector-1", "index-1", 1, Metric.COSINE);

        instance1.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        assertThatNoException().isThrownBy(() -> instance1.getConfig().addVectorCollectionConfig(vectorCollectionConfig));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void memberTest_setVectorCollection_then_fail() {
        String vectorCollection = "vector-collection-1";
        var vectorCollectionConfig = buildVectorCollectionConfig(vectorCollection, "index-1", 1, Metric.COSINE);
        instance1.getConfig().setVectorCollectionConfigs(Map.of(vectorCollectionConfig.getName(), vectorCollectionConfig));
    }

    @Test
    public void memberTest_addBackupCountOneConfig_then_success() {
        memberTest_addBackupConfig("vector-collection-1", 1, null);
        var actual = instance2.getConfig().getVectorCollectionConfigOrNull("vector-collection-1");
        assertThat(actual.getBackupCount()).isEqualTo(1);
    }

    @Test
    public void memberTest_addBackupCountMaxConfig_then_success() {
        memberTest_addBackupConfig("vector-collection-1", 6, null);
        var actual = instance2.getConfig().getVectorCollectionConfigOrNull("vector-collection-1");
        assertThat(actual.getBackupCount()).isEqualTo(6);
    }

    @Test
    public void memberTest_addBackupCountMinConfig_then_success() {
        memberTest_addBackupConfig("vector-collection-1", 0, null);
        var actual = instance2.getConfig().getVectorCollectionConfigOrNull("vector-collection-1");
        assertThat(actual.getBackupCount()).isEqualTo(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void memberTest_addBackupCountMoreThanMaxConfig_then_fail() {
        memberTest_addBackupConfig("vector-collection-1", 7, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void memberTest_addBackupCountLessThanMinConfig_then_fail() {
        memberTest_addBackupConfig("vector-collection-1", -1, null);
    }

    @Test
    public void memberTest_addAsyncBackupCountOneConfig_then_success() {
        memberTest_addBackupConfig("vector-collection-1", null, 1);
        var actual = instance2.getConfig().getVectorCollectionConfigOrNull("vector-collection-1");
        assertThat(actual.getAsyncBackupCount()).isEqualTo(1);
    }

    @Test
    public void memberTest_addAsyncBackupCountMaxConfig_then_success() {
        memberTest_addBackupConfig("vector-collection-1", 0, 6);
        var actual = instance2.getConfig().getVectorCollectionConfigOrNull("vector-collection-1");
        assertThat(actual.getAsyncBackupCount()).isEqualTo(6);
    }

    @Test
    public void memberTest_addAsyncBackupCountMinConfig_then_success() {
        memberTest_addBackupConfig("vector-collection-1", null, 0);
        var actual = instance2.getConfig().getVectorCollectionConfigOrNull("vector-collection-1");
        assertThat(actual.getAsyncBackupCount()).isEqualTo(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void memberTest_addAsyncBackupCountMoreThanMaxConfig_then_fail() {
        memberTest_addBackupConfig("vector-collection-1", 0, 7);
    }

    @Test(expected = IllegalArgumentException.class)
    public void memberTest_addAsyncBackupCountLessThanMinConfig_then_fail() {
        memberTest_addBackupConfig("vector-collection-1", null, -1);
    }

    @Test
    public void memberTest_addSyncAndAsyncBackupCountMaxConfig_then_success() {
        memberTest_addBackupConfig("vector-collection-1", 4, 2);
        var actual = instance2.getConfig().getVectorCollectionConfigOrNull("vector-collection-1");
        assertThat(actual.getBackupCount()).isEqualTo(4);
        assertThat(actual.getAsyncBackupCount()).isEqualTo(2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void memberTest_addSyncAndAsyncBackupCountMoreThanMaxConfig_then_fail() {
        memberTest_addBackupConfig("vector-collection-1", 4, 3);
    }

    private void memberTest_addBackupConfig(String name, Integer backupCount, Integer asyncBackupCount) {
        var vectorCollectionConfig
                = buildVectorCollectionForBackupTests(name, backupCount, asyncBackupCount);
        instance1.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
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

    // client test fails because codecs are registered in EE module
    @Test(expected = UnsupportedOperationException.class)
    public void clientTest_addAndGetVectorCollectionConfig_then_fail_in_os() {
        var vectorCollectionConfig1 = buildVectorCollectionConfig(
                "vector_collection-1",
                "index-1-1",
                3,
                Metric.COSINE,
                11,
                12,
                true
        );
        client.getConfig().addVectorCollectionConfig(vectorCollectionConfig1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void clientTest_setVectorCollection_then_failed() {
        client.getConfig().setVectorCollectionConfigs(Collections.emptyMap());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void clientTest_getAllVectorCollectionConfigs_then_failed() {
        client.getConfig().getVectorCollectionConfigs();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void clientTest_getVectorCollectionConfig_then_failed() {
        client.getConfig().getVectorCollectionConfigOrNull("any");
    }
}
