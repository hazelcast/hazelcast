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

package com.hazelcast.vector.impl.proxy;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.concurrent.CompletionStage;

import static com.hazelcast.vector.impl.VectorTestUtils.sr;
import static com.hazelcast.vector.impl.VectorTestUtils.usingOverriddenEqualsIgnoringFields;
import static com.hazelcast.vector.impl.VectorTestUtils.vec;
import static com.hazelcast.vector.impl.proxy.VectorCollectionProxyTest.DOC_1D_NEGATIVE;
import static com.hazelcast.vector.impl.proxy.VectorCollectionProxyTest.DOC_1D_POSITIVE;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorCollectionSearchConsistencyTest extends HazelcastTestSupport {

    private final String collectionName = randomName();
    private String indexName = "index1";

    protected TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance[] members;
    private HazelcastInstance member;

    @Parameterized.Parameters(name = "operation={4}: clusterSize={0},deduplication={1},withRecreateIndex={2},includeVectors={3}")
    public static List<Object[]> parameters() {
        return cartesianProduct(List.of(1, 2), List.of(false, true), List.of(false, true), List.of(false, true),
                List.of(MutatingFunction.values())
        );
    }

    // remove is internally implemented using get+delete, so it is not tested here
    // putAll is translated to set, so also not separately tested here
    public enum MutatingFunction implements TriFunction<VectorCollection<String, String>, String, VectorDocument<String>, CompletionStage<?>> {
        SET {
            @Override
            public CompletionStage<?> applyEx(VectorCollection<String, String> vectorCollection, String key, VectorDocument<String> value) {
                return vectorCollection.setAsync(key, value);
            }
        },
        PUT {
            @Override
            public CompletionStage<?> applyEx(VectorCollection<String, String> vectorCollection, String key, VectorDocument<String> value) {
                return vectorCollection.putAsync(key, value);
            }
        },
        DELETE_AND_PUT {
            @Override
            public CompletionStage<?> applyEx(VectorCollection<String, String> vectorCollection, String key, VectorDocument<String> value) {
                return vectorCollection.deleteAsync(key).thenComposeAsync(__ -> vectorCollection.putAsync(key, value));
            }
        },
        DELETE_AND_SET {
            @Override
            public CompletionStage<?> applyEx(VectorCollection<String, String> vectorCollection, String key, VectorDocument<String> value) {
                return vectorCollection.deleteAsync(key).thenComposeAsync(__ -> vectorCollection.setAsync(key, value));
            }
        },
        DELETE_AND_PUT_IF_ABSENT {
            @Override
            public CompletionStage<?> applyEx(VectorCollection<String, String> vectorCollection, String key, VectorDocument<String> value) {
                return vectorCollection.deleteAsync(key).thenComposeAsync(__ -> vectorCollection.putIfAbsentAsync(key, value));
            }
        },

    }

    @Parameterized.Parameter
    public int clusterSize;

    @Parameterized.Parameter(1)
    public boolean useDeduplication;

    @Parameterized.Parameter(2)
    public boolean withRecreateIndex;

    /**
     * Value and score must be consistent even if vectors are not returned
     */
    @Parameterized.Parameter(3)
    public boolean includeVectors;

    @Parameterized.Parameter(4)
    public MutatingFunction mutatingOperation;

    @Before
    public void setup() {
        members = factory.newInstances(smallInstanceConfigWithoutJetAndMetrics(), clusterSize);
        member = members[0];
    }

    @After
    public void shutdownFactory() {
        factory.shutdownAll();
    }

    protected HazelcastInstance hz() {
        return member;
    }

    /**
     * This test attempts to trigger a race condition during search, but it is quite hard.
     * For manual tests it is helpful to add 1ms delays in appropriate places, eg. in mutating
     * operation after value is updated but before vectors update or between different
     * phases of search.
     * During CI runs any flakiness of this test indicates a potential race condition.
     */
    @Test
    public void testSearchWithConcurrentUpdateReturnsConsistentResults() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        final int partitionId = 0;
        String dummyEntryKey = generateKeyForPartition(member, "entry-", partitionId);
        // if the partition contains only 1 entry, the index will be recreated when it is updated
        // (see maybeRecreateIndex), otherwise the index is not recreated.
        String updatedEntryKey = generateKeyForPartition(member, "updated-", partitionId + (withRecreateIndex ? 1 : 0));
        vectorCollection.putAsync(dummyEntryKey, VectorDocument.of("entry", vec(3.5f))).toCompletableFuture().join();
        vectorCollection.putAsync(updatedEntryKey, DOC_1D_NEGATIVE).toCompletableFuture().join();

        int dummiesFound = 0;
        int emptyResults = 0;
        final int iterations = 10_000;

        for (int i = 0; i < iterations; i++) {
            // concurrent update and search
            // it tries to trigger race condition when search is executed after update of KV store
            // but before update of vector index during put invocation
            //
            // Search is invoked _before_ put to cover also single-stage searcher for 1-member cluster.
            // In case of single-stage search, the SearchOperation goes through partition thread before
            // being offloaded so it would not see partial write of preceding put, which is executed
            // fully on partition thread. This is not an issue for two-stage searcher because
            // SearchMemberOperation executes on generic thread and offloads.

            var searchFuture = vectorCollection.searchAsync(vec(0.5f),
                    SearchOptions.builder().limit(1).includeValue().setIncludeVectors(includeVectors).build()
                    ).toCompletableFuture();
            var putFuture = mutatingOperation.apply(vectorCollection, updatedEntryKey, i % 2 == 0 ? DOC_1D_POSITIVE : DOC_1D_NEGATIVE).toCompletableFuture();
            putFuture.join();
            var results = searchFuture.join();

            assertThat(results.results()).toIterable()
                    .usingRecursiveFieldByFieldElementComparator(usingOverriddenEqualsIgnoringFields("id"))
                    .as("Should return consistent value in search results in iteration %d", i)
                    .isSubsetOf(
                            // results with consistent value and vector
                            sr(updatedEntryKey, 1f, "positive", includeVectors ? vec(0.5f) : null),
                            sr(updatedEntryKey, 0.5f, "negative", includeVectors ? vec(-0.5f) : null),
                            // can happen if search traverses the index when old vector has already been removed
                            // but a new one has not yet been added. this is acceptable.
                            sr(dummyEntryKey, 0.1f, "entry", includeVectors ? vec(3.5f) : null)
                    );

            // empty results can be returned only with index recreation
            // in setup without recreation, there is always a second partition with dummy result
            if (!withRecreateIndex && results.size() == 0) {
                emptyResults++;
            } else {
                assertThat(results.results()).toIterable()
                        .as("Should return single result in iteration %d", i)
                        .hasSize(1);
            }
            if (results.size() > 0 && dummyEntryKey.equals(results.results().next().getKey())) {
                dummiesFound++;
            }
        }

        System.out.println("Dummies found: " + dummiesFound);
        if (!withRecreateIndex) {
            System.out.println("Empty results found: " + emptyResults);
            assertThat(emptyResults).as("Usually should find some entry").isLessThan(iterations / 10);
        }
        assertThat(dummiesFound).as("Should find actual entry sometimes").isLessThan(iterations);
    }

    private <T> VectorCollection<T, T> getVectorCollectionWith1Dim(HazelcastInstance member) {
        VectorIndexConfig vectorIndexConfig = new VectorIndexConfig()
                .setMetric(Metric.EUCLIDEAN)
                .setDimension(1)
                .setUseDeduplication(useDeduplication);
        if (indexName != null) {
            vectorIndexConfig.setName(indexName);
        }
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(collectionName)
                .addVectorIndexConfig(vectorIndexConfig);
        member.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        return member.getVectorCollection(vectorCollectionConfig.getName());
    }
}
