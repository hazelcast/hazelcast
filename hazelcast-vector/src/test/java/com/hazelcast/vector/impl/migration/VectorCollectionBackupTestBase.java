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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.VectorDocument;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.util.Util.memoize;
import static com.hazelcast.vector.impl.VectorTestUtils.randomVec;
import static com.hazelcast.vector.impl.VectorTestUtils.warmupOneIndexCollection;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class VectorCollectionBackupTestBase extends VectorCollectionMigrationTestBase {

    @Parameterized.Parameters(name = "useClient={2}")
    public static List<Object[]> parameters() {
        return cartesianProduct(List.of(false), List.of(false), List.of(false, true));
    }

    @Parameterized.Parameter(2)
    public boolean useClient;

    private Supplier<HazelcastInstance> clientSupplier = memoize(() -> factory.newHazelcastClient());

    protected HazelcastInstance hz() {
        return useClient ? clientSupplier.get() : members[0];
    }

    @Before
    public void postSetup() {
        collection.clearAsync().toCompletableFuture().join();
        collection = hz().getVectorCollection(collectionName);
    }

    @Test
    public void testPut() {
        runPutScenario(collection::putAsync);
    }

    @Test
    public void testPutIfAbsent() {
        runPutScenario(collection::putIfAbsentAsync);
    }

    @Test
    public void testSet() {
        runPutScenario(collection::setAsync);
    }

    @Test
    public void testPutAllOne() {
        runPutScenario((k, v) -> collection.putAllAsync(Map.of(k, v)));
    }

    private void runPutScenario(BiFunction<String, VectorDocument<String>, CompletionStage<?>> f) {
        runScenario(instance -> {
                    var key = generateKeyOwnedBy(instance);
                    VectorDocument<String> doc = VectorDocument.of(key, randomVec(DIMENSION));
                    assertThat(f.apply(key, doc)).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
                    return tuple2(key, doc);
                },
                state -> {
                    assertThat(collection.size()).isOne();
                    assertThat(collection.getAsync(state.getKey()))
                            .succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION)
                            .isEqualTo(state.getValue());
                    assertThatAllEntriesCanBeSearched(1);
                }
        );
    }

    @Test
    public void testPutAllSinglePartition() {
        runScenario(instance -> {
                    var entries = range(0, 100).mapToObj(i -> {
                        var key = generateKeyOwnedBy(instance);
                        return Map.entry(key, VectorDocument.of(key, randomVec(DIMENSION)));
                    }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    assertThat(collection.putAllAsync(entries)).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
                    return entries;
                },
                state -> {
                    assertThat(collection.size()).isEqualTo(state.size());
                    state.forEach((k, v) ->
                            assertThat(collection.getAsync(k))
                                    .succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION)
                                    .isEqualTo(v));
                    assertThatAllEntriesCanBeSearched(state.size());
                }
        );
    }

    @Test
    public void testPutAll() {
        runScenario(instance -> {
                    var entries = range(0, 100)
                            .mapToObj(key -> Map.entry("" + key, VectorDocument.of("" + key, randomVec(DIMENSION))))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    assertThat(collection.putAllAsync(entries)).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
                    return entries;
                },
                state -> {
                    assertThat(collection.size()).isEqualTo(state.size());
                    state.forEach((k, v) ->
                            assertThat(collection.getAsync(k))
                                    .succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION)
                                    .isEqualTo(v));
                    assertThatAllEntriesCanBeSearched(state.size());
                }
        );
    }

    @Test
    public void testRemove() {
        runRemoveScenario(collection::removeAsync);
    }

    @Test
    public void testDelete() {
        runRemoveScenario(collection::deleteAsync);
    }

    private void runRemoveScenario(Function<String, CompletionStage<?>> f) {
        runScenario(instance -> {
                    var key = generateKeyOwnedBy(instance);
                    VectorDocument<String> doc = VectorDocument.of(key, randomVec(DIMENSION));
                    collection.putAsync(key, doc).toCompletableFuture().get();
                    return tuple2(key, doc);
                },
                (instance, state) -> {
                    assertThat(f.apply(state.getKey())).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
                    return state;
                },
                state -> {
                    assertThat(collection.size()).isZero();
                    assertThat(collection.getAsync(state.getKey()))
                            .succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION)
                            .isNull();
                    assertThatAllEntriesCanBeSearched(0);
                }
        );
    }

    @Test
    public void testClear() {
        warmupOneIndexCollection(members[0], collection);

        runScenario(instance -> {
                    assertThat(collection.size()).isPositive();
                    assertThat(collection.clearAsync()).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
                    return null;
                },
                state -> assertThat(collection.size()).isZero()
        );
    }

    @Test
    public void testOptimize() {
        // checks if optimize finishes when failure occurs during its execution
        final int iterations = 100;
        for (int i = 0; i < iterations; ++i) {
            warmupOneIndexCollection(members[0], collection);
        }

        runScenario(instance -> {
                    assertThat(collection.size()).isPositive();
                    return collection.optimizeAsync();
                },
                optimizeFuture -> {
                    assertThat(optimizeFuture).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
                    assertThat(collection.size()).isEqualTo((long) iterations * partitionCount);
                }
        );
    }

    @Test
    public void testOptimizeAndMutationsDuringFailure() {
        // checks if mutating operations are not lost when they wait for optimisation to finish
        int iterations = 100;
        for (int i = 0; i < iterations; ++i) {
            warmupOneIndexCollection(members[0], collection);
        }

        runScenario(instance -> {
                    assertThat(collection.size()).isPositive();
                    CompletionStage<Void> optimizeFuture = collection.optimizeAsync();
                    warmupOneIndexCollection(members[0], collection);
                    return optimizeFuture;
                },
                optimizeFuture -> {
                    assertThat(optimizeFuture).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
                    assertThat(collection.size()).isEqualTo((long) (iterations + 1) * partitionCount);
                }
        );
    }

    /**
     * @param init       initialization to execute before failure conditions
     * @param action     action to execute during potential failure conditions
     * @param validation validation to check that no data was lost
     * @param <S>        state passed between init and action if needed
     * @param <T>        state passed between action and validation if needed
     */
    protected abstract <S, T> void runScenario(FunctionEx<HazelcastInstance, S> init,
                                               BiFunctionEx<HazelcastInstance, S, T> action,
                                               ConsumerEx<T> validation);

    protected <S> void runScenario(FunctionEx<HazelcastInstance, S> action,
                                   ConsumerEx<S> validation) {
        runScenario(i -> null, (i, s) -> action.apply(i), validation);
    }

    protected void assertThatAllEntriesCanBeSearched(int expected) {
        assertThat(collection.searchAsync(randomVec(DIMENSION),
                // limit is artificially increased to handle skewed distribution in some tests
                SearchOptions.builder().limit(partitionCount * Math.max(1, expected)).build()))
                .succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION)
                .extracting(SearchResults::size).isEqualTo(expected);
    }
}
