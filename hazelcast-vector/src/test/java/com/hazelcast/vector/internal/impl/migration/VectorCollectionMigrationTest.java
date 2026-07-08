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
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.vector.VectorCollection;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.hazelcast.vector.internal.impl.VectorTestUtils.warmupOneIndexCollection;
import static org.assertj.core.api.Assumptions.assumeThat;

@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorCollectionMigrationTest extends VectorCollectionMigrationTestBase {

    @Test
    public void vectorCollectionDataAreMigrated_whenMemberGracefulShutdown() {
        assertSize(members[0]);
        members[2].shutdown();
        waitClusterForSafeState(members[0]);
        assertSize(members[0]);
        assertAllPartitionsMutable(members[0]);
    }

    @Test
    public void vectorCollectionDataAreMigrated_whenMasterGracefulShutdown() {
        assertSize(members[0]);
        members[0].shutdown();
        waitClusterForSafeState(members[1]);
        assertSize(members[1]);
        assertAllPartitionsMutable(members[1]);
    }

    @Test
    public void vectorCollectionDataCanBeMigratedTwice() {
        assertSize(members[0]);
        members[2].shutdown();
        waitClusterForSafeState(members[0]);
        // some partitions should be migrated from member 2 to 1 and then to 0
        members[1].shutdown();
        waitClusterForSafeState(members[0]);
        assertSize(members[0]);
        assertAllPartitionsMutable(members[0]);
    }

    @Test
    public void vectorCollectionDataAreMigrated_whenMemberAdded() {
        assertSize(members[0]);
        var newMember = factory.newHazelcastInstance(getConfig());
        waitAllForSafeState(members[0], members[1], members[2], newMember);
        assertSize(members[1]);
        assertSize(newMember);
        assertAllPartitionsMutable(members[0]);
        assertAllPartitionsMutable(newMember);
    }

    @Test
    public void vectorCollectionDataAreMigrated_whenSomeEntriesWereDeleted() {
        touchAllPartitions(members[0]);
        assertSize(members[0]);
        members[2].shutdown();
        waitClusterForSafeState(members[0]);
        assertSize(members[0]);
        assertAllPartitionsMutable(members[0]);
    }

    @Test
    public void vectorCollectionDataAreMigrated_whenManyCollections() {
        String collectionName2 = collectionName + "2";
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(collectionName2)
                .addVectorIndexConfig(new VectorIndexConfig(indexName, Metric.EUCLIDEAN, DIMENSION)
                        .setUseDeduplication(useDeduplication));
        members[0].getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        VectorCollection<String, String> collection2 = members[0].getVectorCollection(vectorCollectionConfig.getName());
        warmupOneIndexCollection(members[0], collection2);

        assertSize(members[0], collectionName);
        assertSize(members[0], collectionName2);
        members[2].shutdown();
        waitClusterForSafeState(members[0]);
        assertSize(members[0], collectionName);
        assertSize(members[0], collectionName2);
        assertAllPartitionsMutable(members[0], collectionName);
        assertAllPartitionsMutable(members[0], collectionName2);
    }

    @Test
    public void vectorCollectionDataAreMigrated_whenProxyDoesNotExist() {
        var member = members[0];
        VectorCollection<String, String> collection = member.getVectorCollection(collectionName);
        // destroy proxy, usage after destroy is not recommended but is needed for the test
        collection.destroy();
        // be careful not to recreate proxy before migration
        warmupOneIndexCollection(members[0], collection);
        assertSize(collection);
        members[2].shutdown();
        waitClusterForSafeState(members[0]);
        assertSize(members[0]);
        assertAllPartitionsMutable(members[0]);
    }

    @Test
    public void vectorCollectionDataAreAvailable_whenMemberTerminates() {
        assumeThat(getBackupCount() + getAsyncBackupCount())
                .as("Data survives termination when there are backups")
                .isPositive();

        assertSize(members[0]);

        if (getBackupCount() == 0) {
            // if there are only async backups they could be lost or delayed leading to
            // data loss and test flakiness
            waitClusterForSafeState(members[0]);
        }

        members[2].getLifecycleService().terminate();
        waitClusterForSafeState(members[0]);
        assertSize(members[0]);
        assertAllPartitionsMutable(members[0]);
    }
}
