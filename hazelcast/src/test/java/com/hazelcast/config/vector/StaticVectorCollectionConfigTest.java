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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.stream.Collectors;

import static com.hazelcast.config.vector.VectorTestHelper.buildVectorCollectionConfig;
import static java.util.function.Function.identity;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StaticVectorCollectionConfigTest extends HazelcastTestSupport {

    @Test
    public void setOneVectorCollectionConfig() {
        String collectionName = "vector-collection-1";
        var vectorCollectionConfig = buildVectorCollectionConfig(collectionName, "index-1", 2, Metric.EUCLIDEAN);
        Config config = new Config().addVectorCollectionConfig(vectorCollectionConfig);

        HazelcastInstance instance = createHazelcastInstance(config);

        var actual = instance.getConfig().getVectorCollectionConfigOrNull(collectionName);
        assertThat(actual).isEqualTo(vectorCollectionConfig);
    }

    @Test
    public void setSeveralVectorCollectionConfig() {
        var vectorCollectionConfigs = range(0, 3)
                .mapToObj(i -> buildVectorCollectionConfig("collection-" + i, "index-" + i, i + 1, Metric.EUCLIDEAN)
                        .setBackupCount(i).setAsyncBackupCount(i))
                .collect(Collectors.toMap(VectorCollectionConfig::getName, identity()));
        Config config = new Config().setVectorCollectionConfigs(vectorCollectionConfigs);

        HazelcastInstance instance = createHazelcastInstance(config);

        var actual = instance.getConfig().getVectorCollectionConfigs();
        assertThat(actual.entrySet()).containsExactlyInAnyOrderElementsOf(vectorCollectionConfigs.entrySet());
    }

    @Test
    public void memberTest_addAndGetWildcardVectorCollection_then_success() {
        String collectionName = "vector-collection-*";
        var vectorCollectionConfig = buildVectorCollectionConfig(collectionName, "index-1", 2, Metric.EUCLIDEAN);
        Config config = new Config().addVectorCollectionConfig(vectorCollectionConfig);

        HazelcastInstance instance = createHazelcastInstance(config);

        var actual = instance.getConfig().getVectorCollectionConfigOrNull("vector-collection-123");
        assertThat(actual).isEqualTo(vectorCollectionConfig);
    }
}
