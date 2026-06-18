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

package com.hazelcast.vector.impl.migration;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static com.hazelcast.vector.impl.VectorTestUtils.randomVec;
import static com.hazelcast.vector.impl.VectorTestUtils.warmupOneIndexCollection;
import static com.hazelcast.vector.impl.proxy.VectorCollectionProxyTest.TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
public abstract class VectorCollectionMigrationTestBase extends HazelcastTestSupport {

    protected static final int DIMENSION = 2;

    protected TestHazelcastFactory factory;
    protected HazelcastInstance[] members;
    protected int partitionCount;

    protected String collectionName = randomName();
    protected String indexName;
    protected VectorCollection<String, String> collection;

    @Parameterized.Parameter
    public boolean useDeduplication;

    @Parameterized.Parameter(1)
    public boolean namedIndex;

    protected int getNodeCount() {
        return 3;
    }

    protected int getBackupCount() {
        return VectorCollectionConfig.DEFAULT_BACKUP_COUNT;
    }

    protected int getAsyncBackupCount() {
        return 0;
    }

    protected int getTotalBackupCount() {
        return getBackupCount() + getAsyncBackupCount();
    }

    @Parameterized.Parameters(name = "deduplication={0}, namedIndex={1}")
    public static List<Object[]> parameters() {
        return cartesianProduct(List.of(false, true), List.of(false, true));
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfigWithoutJetAndMetrics();
    }

    @Before
    public void setup() {
        factory = new TestHazelcastFactory();
        members = factory.newInstances(getConfig(), getNodeCount());
        partitionCount = members[0].getPartitionService().getPartitions().size();

        // wait for cluster fully connected (safe state) before tests
        // to avoid flakiness in tests with small INVOCATION_MAX_RETRY_COUNT
        assertClusterSizeEventually(getNodeCount(), members);
        waitAllForSafeState(members);

        indexName = namedIndex ? "default" : null;
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(collectionName)
                .setBackupCount(getBackupCount()).setAsyncBackupCount(getAsyncBackupCount())
                .addVectorIndexConfig(new VectorIndexConfig(indexName, Metric.EUCLIDEAN, DIMENSION)
                        .setUseDeduplication(useDeduplication));
        members[0].getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        collection = members[0].getVectorCollection(vectorCollectionConfig.getName());
        warmupOneIndexCollection(members[0], collection);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    protected void assertSize(HazelcastInstance member) {
        assertSize(member, collectionName);
    }

    protected void assertSize(HazelcastInstance member, String name) {
        VectorCollection<?, ?> collection = member.getVectorCollection(name);
        assertSize(collection);
    }

    protected void assertSize(VectorCollection<?, ?> collection) {
        // check both KV-reported size and vectors in the index
        assertThat(collection.size()).isEqualTo(partitionCount);
        var stage = collection.searchAsync(
                namedIndex ? VectorValues.of(indexName, new float[]{0, 0}) : VectorValues.of(new float[]{0, 0}),
                SearchOptions.builder().limit(partitionCount).build());
        var results = stage.toCompletableFuture().join();
        assertEquals(partitionCount, results.size());
    }

    /**
     * Ensures that all partitions require cleanup before migration
     */
    protected void touchAllPartitions(HazelcastInstance member) {
        VectorCollection<String, String> collection = member.getVectorCollection(collectionName);
        touchAllPartitions(member, collection);
    }

    protected void touchAllPartitions(HazelcastInstance member, VectorCollection<String, String> collection) {
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            String key = generateKeyForPartition(member, partitionId);
            assertThat(collection.putAsync(key, VectorDocument.of("value", randomVec(DIMENSION)))).succeedsWithin(TIMEOUT);
            assertThat(collection.removeAsync(key)).succeedsWithin(TIMEOUT).isNotNull();
        }
    }

    protected void assertAllPartitionsMutable(HazelcastInstance member) {
        assertAllPartitionsMutable(member, collectionName);
    }

    protected void assertAllPartitionsMutable(HazelcastInstance member, String name) {
        VectorCollection<String, String> collection = member.getVectorCollection(name);
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            String key = generateKeyForPartition(member, partitionId);
            assertThat(collection.putAsync(key, VectorDocument.of("value", randomVec(DIMENSION)))).succeedsWithin(TIMEOUT);
            assertThat(collection.getAsync(key)).succeedsWithin(TIMEOUT).isNotNull();
        }
    }
}
