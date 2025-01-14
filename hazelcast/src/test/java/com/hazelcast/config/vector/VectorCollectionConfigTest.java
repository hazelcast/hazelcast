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

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static com.hazelcast.config.vector.VectorCollectionConfig.MAX_BACKUP_COUNT;
import static com.hazelcast.config.vector.VectorTestHelper.buildVectorCollectionConfig;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorCollectionConfigTest {

    @Test
    public void constructorNameValidation_failed() {
        assertThatThrownBy(() -> new VectorCollectionConfig("asd%6(&"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The name of the vector collection should "
                        + "only consist of letters, numbers, and the symbols \"-\", \"_\" or \"*\".");
    }

    @Test
    public void setNameValidation_failed() {
        assertThatThrownBy(() -> new VectorCollectionConfig().setName("asd*^"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The name of the vector collection should "
                        + "only consist of letters, numbers, and the symbols \"-\", \"_\" or \"*\".");
    }

    @Test
    public void constructorNameValidation_success() {
        assertThatNoException().isThrownBy(() -> new VectorCollectionConfig("***ve*c_tor-234-ANY_*"));
    }

    @Test
    public void setNameValidation_success() {
        assertThatNoException().isThrownBy(() -> new VectorCollectionConfig().setName("***ve*c_tor-234-ANY_*"));
    }

    @Test
    public void shouldValidateBackupCount() {
        assertThatNoException().isThrownBy(() ->
                new VectorCollectionConfig("a").setBackupCount(0).setAsyncBackupCount(0));
        assertThatNoException().isThrownBy(() ->
                new VectorCollectionConfig("a").setBackupCount(MAX_BACKUP_COUNT).setAsyncBackupCount(0));
        assertThatNoException().isThrownBy(() ->
                new VectorCollectionConfig("a").setBackupCount(0).setAsyncBackupCount(MAX_BACKUP_COUNT));

        assertThatThrownBy(() -> new VectorCollectionConfig("a").setBackupCount(MAX_BACKUP_COUNT).setAsyncBackupCount(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("the sum of backup-count and async-backup-count can't be larger than than 6");
        assertThatThrownBy(() -> new VectorCollectionConfig("a").setAsyncBackupCount(1).setBackupCount(MAX_BACKUP_COUNT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("the sum of backup-count and async-backup-count can't be larger than than 6");
    }

    @Test
    public void shouldValidateMergePolicy() {
        assertThatThrownBy(() -> new VectorCollectionConfig("a").setMergePolicyConfig(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void addTwoIndexWithTheSameName_then_fail() {
        String indexName = "index-1";
        var vectorCollectionConfig = buildVectorCollectionConfig("collectionName", indexName, 2, Metric.EUCLIDEAN);
        VectorIndexConfig indexConfig = new VectorIndexConfig().setName(indexName);
        assertThatThrownBy(() -> vectorCollectionConfig.addVectorIndexConfig(indexConfig))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("The vector index configuration contains multiple indexes with the same name");
    }

    @Test
    public void addTwoIndexWithTheEmptyName_then_fail() {
        var vectorCollectionConfig = new VectorCollectionConfig();
        vectorCollectionConfig.addVectorIndexConfig(new VectorIndexConfig());
        assertThatThrownBy(() -> vectorCollectionConfig.addVectorIndexConfig(new VectorIndexConfig()))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("The vector index configuration contains multiple indexes with the same name");
    }

    @Test
    public void setTwoIndexWithTheSameName_then_fail() {
        String indexName = "index-1";
        var vectorCollectionConfig = buildVectorCollectionConfig("collectionName", indexName, 2, Metric.EUCLIDEAN);
        VectorIndexConfig indexConfig1 = new VectorIndexConfig().setName(indexName);
        VectorIndexConfig indexConfig2 = new VectorIndexConfig().setName(indexName);
        assertThatThrownBy(
                () -> vectorCollectionConfig.setVectorIndexConfigs(List.of(indexConfig1, indexConfig2))
        ).isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("The vector index configuration contains multiple indexes with the same name");
    }

    @Test
    public void setTwoIndexWithEmptyName_then_fail() {
        var vectorCollectionConfig = buildVectorCollectionConfig("collectionName", "indexName", 2, Metric.EUCLIDEAN);
        VectorIndexConfig indexConfig1 = new VectorIndexConfig();
        VectorIndexConfig indexConfig2 = new VectorIndexConfig();
        assertThatThrownBy(
                () -> vectorCollectionConfig.setVectorIndexConfigs(List.of(indexConfig1, indexConfig2))
        ).isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("The vector index configuration contains multiple indexes with the same name");
    }

    @Test
    public void setNamedAndUnnamedIndex_then_fail() {
        var vectorCollectionConfig = new VectorCollectionConfig("collection");
        VectorIndexConfig indexConfig1 = new VectorIndexConfig().setName("name");
        VectorIndexConfig indexConfig2 = new VectorIndexConfig();
        assertThatThrownBy(
                () -> vectorCollectionConfig.setVectorIndexConfigs(List.of(indexConfig1, indexConfig2))
        ).isInstanceOf(InvalidConfigurationException.class)
                .hasMessage("Vector collection cannot contain both named and unnamed index");
    }

    @Test
    public void addNamedAndUnnamedIndex_then_fail() {
        var vectorCollectionConfig = new VectorCollectionConfig("collection");
        vectorCollectionConfig.addVectorIndexConfig(new VectorIndexConfig().setName("name"));
        VectorIndexConfig indexConfig2 = new VectorIndexConfig();
        assertThatThrownBy(
                () -> vectorCollectionConfig.addVectorIndexConfig(indexConfig2)
        ).isInstanceOf(InvalidConfigurationException.class)
                .hasMessage("Vector collection cannot contain both named and unnamed index");
    }
}
