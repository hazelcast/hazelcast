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

package com.hazelcast.vector.internal.impl.migration;

import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.util.List;

import static com.hazelcast.vector.internal.impl.VectorTestUtils.warmupOneIndexCollection;

@Category({NightlyTest.class, ParallelJVMTest.class})
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class VectorCollectionMigrationLeakTest extends VectorCollectionMigrationTestBase {
    @Parameterized.Parameters(name = "deduplication={0}, namedIndex={1}")
    public static List<Object[]> parameters() {
        return cartesianProduct(List.of(false, true), List.of(false));
    }

    @Test
    public void vectorCollectionMigrationMemoryLeak() {
        // use many smaller collections to trigger OOM faster
        for (int collNo = 0; collNo < 2_000; ++collNo) {
            VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(collectionName + collNo)
                    .addVectorIndexConfig(new VectorIndexConfig(indexName, Metric.EUCLIDEAN, DIMENSION)
                            .setUseDeduplication(useDeduplication));
            members[0].getConfig().addVectorCollectionConfig(vectorCollectionConfig);
            collection = members[0].getVectorCollection(vectorCollectionConfig.getName());
            warmupOneIndexCollection(members[0], collection);
        }

        // It takes about 50 iterations to fill 4GB heap and 90 for 8GB heap with JVector 2.0.5
        for (int i = 0; i < 150; ++i) {
            System.out.println("Migrate: " + i);
            var newMember = factory.newHazelcastInstance(getConfig());
            waitAllForSafeState(members[0], members[1], members[2], newMember);
            assertSize(members[0]);
            assertSize(members[1]);
            assertSize(newMember);
            newMember.shutdown();
            waitAllForSafeState(members);
        }
    }
}
