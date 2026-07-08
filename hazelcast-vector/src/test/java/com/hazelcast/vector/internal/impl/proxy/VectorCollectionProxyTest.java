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
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchResult;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.internal.impl.Hints;
import com.hazelcast.vector.internal.impl.VectorCollectionService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hazelcast.vector.internal.impl.VectorTestUtils.randomVec;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.sr;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.toMap;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.usingOverriddenEqualsIgnoringFields;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.vec;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.warmupOneIndexCollection;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.warmupTwoIndexCollection;
import static java.util.Comparator.comparing;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorCollectionProxyTest extends HazelcastTestSupport {

    public static final VectorDocument<String> DOC_1D_POSITIVE = VectorDocument.of("positive", vec(0.5f));
    public static final VectorDocument<String> DOC_1D_NEGATIVE = VectorDocument.of("negative", vec(-0.5f));

    public static final Duration TIMEOUT = Duration.of(ASSERT_TRUE_EVENTUALLY_TIMEOUT, ChronoUnit.SECONDS);

    private final String collectionName = randomName();
    private String indexName = "index1";
    private final String indexName2 = randomName();
    private final VectorDocument<String> DOC_1D_POSITIVE_NAMED = com.hazelcast.vector.VectorDocument.of("positive", vec(indexName, 0.5f));
    private final VectorDocument<String> DOC_1D_NEGATIVE_NAMED = VectorDocument.of("negative", vec(indexName, -0.5f));

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

    @Test
    public void testInstantiation() {
        assertNotNull(getVectorCollectionWith1Dim(hz()));
        var collection = hz().getVectorCollection(collectionName);
        assertThat(collection).isNotNull();
        assertThat(collection.getName()).isEqualTo(collectionName);
    }

    @Test
    public void testInstantiationUnnamedIndex() {
        indexName = null;
        assertNotNull(getVectorCollectionWith1Dim(hz()));
        var collection = hz().getVectorCollection(collectionName);
        assertThat(collection).isNotNull();
        assertThat(collection.getName()).isEqualTo(collectionName);
    }

    @Test
    public void testInstantiation2Indexes() {
        assertNotNull(getVectorCollectionWith2Indexes(hz()));
        var collection = hz().getVectorCollection(collectionName);
        assertThat(collection).isNotNull();
        assertThat(collection.getName()).isEqualTo(collectionName);
    }

    @Test
    public void testInstantiationNoIndexInCollection() {
        var badConfig = new VectorCollectionConfig(collectionName);

        assertThatThrownBy(() -> {
            hz().getConfig().addVectorCollectionConfig(badConfig);
            hz().getVectorCollection(badConfig.getName());
        })
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessage("Vector collection must have at least one index.");
    }

    @Test
    public void testGetMissingKey() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isNull();
    }

    @Test
    public void testPut() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE_NAMED);
    }

    @Test
    public void testPutNamed() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE_NAMED)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE_NAMED);
    }

    @Test
    public void testPutExistingItem() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.putAsync("1", DOC_1D_NEGATIVE)).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE_NAMED);
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_NEGATIVE_NAMED);
    }

    @Test
    public void testPutExistingItemNamed() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE_NAMED)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.putAsync("1", DOC_1D_NEGATIVE_NAMED)).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE_NAMED);
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_NEGATIVE_NAMED);
    }

    @Test
    public void testPutExistingItemUnnamedAtomic() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE_NAMED)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.putAsync("1", VectorDocument.of("negative", vec(-0.5f, -1))))
                .failsWithin(TIMEOUT).withThrowableThat().havingRootCause().satisfies(isVectorLengthValidationException());
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE_NAMED);
    }

    @Test
    public void testPutExistingItemNamedAtomic() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE_NAMED)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.putAsync("1", VectorDocument.of("negative", vec(indexName, -0.5f, -1))))
                .failsWithin(TIMEOUT).withThrowableThat().havingRootCause().satisfies(isVectorLengthValidationException());
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE_NAMED);
    }

    @Test
    public void testPutExistingItemTwoIndexesAtomic() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith2Indexes(hz());
        final VectorDocument<String> doc2dPositive = VectorDocument.of("positive", VectorValues.of(indexName, new float[]{0.5f}, indexName2, new float[]{1f}));

        assertThat(vectorCollection.putAsync("1", doc2dPositive)).succeedsWithin(TIMEOUT);
        assertThat(vectorCollection.putAsync("1", VectorDocument.of("negative",
                VectorValues.of(indexName, new float[]{-0.5f}, indexName2, new float[]{-1f, -2f}))))
                .failsWithin(TIMEOUT).withThrowableThat().havingRootCause().satisfies(isVectorLengthValidationException());
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(doc2dPositive);
    }

    private static Consumer<Throwable> isVectorLengthValidationException() {
        return t -> assertThat(t).isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Vector length 2 different than expected for index");
    }

    @Test
    public void testPutUnnamedIndex() {
        indexName = null;
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE);
    }

    @Test
    public void testPutUnnamedIndexNamedVector() {
        indexName = null;
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE_NAMED))
                .failsWithin(TIMEOUT).withThrowableThat().havingRootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .withMessage("Invalid vector names specified, the collection does not contain the requested indexes: [index1]");
    }

    @Test
    public void testPutIfAbsent() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putIfAbsentAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE_NAMED);
    }

    @Test
    public void testPutIfAbsentNamed() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putIfAbsentAsync("1", DOC_1D_POSITIVE_NAMED)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE_NAMED);
    }

    @Test
    public void testPutIfAbsentExisting() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putIfAbsentAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.putIfAbsentAsync("1", DOC_1D_NEGATIVE)).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE_NAMED);
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE_NAMED);
    }

    @Test
    public void testPutIfAbsentExistingNamed() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putIfAbsentAsync("1", DOC_1D_POSITIVE_NAMED)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.putIfAbsentAsync("1", DOC_1D_NEGATIVE_NAMED)).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE_NAMED);
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE_NAMED);
    }

    @Test
    public void testSet() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.setAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE_NAMED);
    }

    @Test
    public void testSetExistingItem() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.setAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT);
        assertThat(vectorCollection.setAsync("1", DOC_1D_NEGATIVE)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_NEGATIVE_NAMED);
    }

    @Test
    public void testDelete() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.deleteAsync("1")).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isNull();
    }


    @Test
    public void testDeleteAll_then_partitionStorageFunctionsCorrectly() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.deleteAsync("1")).succeedsWithin(TIMEOUT).isNull();

        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT).isNull();

        var results = vectorCollection.searchAsync(vec(0.5f), SearchOptions.of(
                10, false, true)).toCompletableFuture().join();

        assertEquals(1, results.size());
    }

    @Test
    public void testDeleteMissingKey() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.deleteAsync("1")).succeedsWithin(TIMEOUT);
    }

    @Test
    public void testDeleteMissingKeyDoesNotAffectOtherEntries() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.deleteAsync("2")).succeedsWithin(TIMEOUT);
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE_NAMED);
    }

    @Test
    public void testDeleteDoesNotAffectOtherEntries() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.putAsync("2", DOC_1D_NEGATIVE)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.deleteAsync("1")).succeedsWithin(TIMEOUT);
        assertThat(vectorCollection.getAsync("2")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_NEGATIVE_NAMED);
    }

    @Test
    public void testRemove() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.removeAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE_NAMED);
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isNull();
    }

    @Test
    public void testRemoveMissingKey() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.removeAsync("1")).succeedsWithin(TIMEOUT).isNull();
    }


    @Test
    public void testSearch() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        vectorCollection.putAsync("1", DOC_1D_POSITIVE).toCompletableFuture().join();
        vectorCollection.putAsync("2", DOC_1D_NEGATIVE).toCompletableFuture().join();

        var results = vectorCollection.searchAsync(vec(0.5f), SearchOptions.of(
                10, false, true)).toCompletableFuture().join();

        assertEquals(2, results.size());
        assertThat(results.results()).toIterable()
                .usingRecursiveFieldByFieldElementComparator(usingOverriddenEqualsIgnoringFields("id"))
                .containsExactly(
                        sr("1", 1f, vec(0.5f)),
                        sr("2", 0.5f, vec(-0.5f)));
    }

    @Test
    public void testSearchMany_oneOfMany() {
        testSearchMany(1000, 1);
    }

    @Test
    public void testSearchMany_someOfMany() {
        // topK less than number of partitions
        testSearchMany(1000, 5);
    }

    @Test
    public void testSearchMany_manyOfMany() {
        // topK more than number of partitions
        testSearchMany(1000, 100);
    }

    @Test
    public void testSearchMany_all() {
        // topK more than size of the collection
        testSearchMany(1000, 2000);
    }

    @Test
    public void testSearchMany_small() {
        // less entries than partitions
        testSearchMany(5, 2);
    }

    @Test
    public void testSearchEmptyCollection() {
        testSearchMany(0, 1);
    }

    private void testSearchMany(int entryCount, int topK) {
        final int expectedSize = Math.min(entryCount, topK);

        // adjust scale of vectors so scores have a reasonable range
        List<Float> inputVectors = range(0, entryCount).mapToObj(v -> v * 10f / entryCount).collect(Collectors.toList());
        Collections.shuffle(inputVectors);
        Float[] expectedVectors = range(0, expectedSize).mapToObj(v -> v * 10f / entryCount).toArray(Float[]::new);

        VectorCollection<Integer, Integer> vectorCollection = getVectorCollectionWith1Dim(hz());

        for (int i = 0; i < entryCount; ++i) {
            vectorCollection.putAsync(i, VectorDocument.of(i, VectorValues.of(new float[]{inputVectors.get(i)})))
                    .toCompletableFuture().join();
        }

        var results = vectorCollection.searchAsync(vec(0), SearchOptions.builder()
                .limit(topK)
                .includeVectors()
                .build()).toCompletableFuture().join();

        assertEquals(expectedSize, results.size());
        var resultsList = Lists.newArrayList(results.results());
        assertThat(resultsList).hasSize(expectedSize);
        assertThat(resultsList).isSortedAccordingTo(comparing((SearchResult<?, ?> sr) -> sr.getScore()).reversed());
        assertThat(resultsList).map(r -> ((VectorValues.SingleVectorValues) r.getVectors()).vector()[0])
                .as("Should find truly closest vectors")
                .containsExactly(expectedVectors);
    }

    @Test
    public void testSearch_withUnnamedIndex() {
        indexName = null;
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        vectorCollection.putAsync("1", DOC_1D_POSITIVE).toCompletableFuture().join();
        vectorCollection.putAsync("2", DOC_1D_NEGATIVE).toCompletableFuture().join();

        var results = vectorCollection.searchAsync(vec(0.5f), SearchOptions.of(
                10, false, true)).toCompletableFuture().join();

        assertEquals(2, results.size());
    }

    @Test
    public void testSearch_withUnnamedIndexByNamedIndex() {
        indexName = null;
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        vectorCollection.putAsync("1", DOC_1D_POSITIVE).toCompletableFuture().join();
        vectorCollection.putAsync("2", DOC_1D_NEGATIVE).toCompletableFuture().join();

        assertThat(vectorCollection.searchAsync(VectorValues.of("missing", new float[] {0.5f}), SearchOptions.of(
                10, false, true))).failsWithin(TIMEOUT).withThrowableThat().havingRootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .withMessage("No vector index named 'missing' is defined");
    }

    @Test
    public void testSearch_withMultipleIndexesByDefaultIndex() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith2Indexes(hz());

        vectorCollection.putAsync("1", VectorDocument.of("test1", VectorValues.of(indexName, new float[]{0.5f}, indexName2, new float[]{-0.5f})))
                .toCompletableFuture().join();
        vectorCollection.putAsync("2", VectorDocument.of("test1", VectorValues.of(indexName, new float[]{-0.5f}, indexName2, new float[]{0.5f})))
                .toCompletableFuture().join();

        assertThat(vectorCollection.searchAsync(vec(0.5f), SearchOptions.of(
                10, false, true))).failsWithin(TIMEOUT).withThrowableThat().havingRootCause()
                .as("Search by default index is not supported for multi-index collections")
                .isInstanceOf(IllegalArgumentException.class)
                .withMessage("Index must be selected for collection with more than 1 index");
    }

    @Test
    public void testSearch_withMultipleIndexesByDefaultIndexByName() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith2Indexes(hz());
        vectorCollection.putAsync("1", VectorDocument.of("test1", VectorValues.of(indexName, new float[]{0.5f}, indexName2, new float[]{-0.5f})))
                .toCompletableFuture().join();
        vectorCollection.putAsync("2", VectorDocument.of("test1", VectorValues.of(indexName, new float[]{-0.5f}, indexName2, new float[]{0.5f})))
                .toCompletableFuture().join();

        var results = vectorCollection.searchAsync(VectorValues.of(indexName, new float[]{0.5f}), SearchOptions.of(
                10, false, true)).toCompletableFuture().join();
        assertEquals(2, results.size());
        assertThat(results.results()).toIterable()
                .usingRecursiveFieldByFieldElementComparator(usingOverriddenEqualsIgnoringFields("id"))
                .containsExactly(
                        sr("1", 1f, vec(0.5f)),
                        sr("2", 0f, vec(-0.5f)));
    }

    @Test
    public void testSearch_withMultipleIndexesByNonDefaultIndex() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith2Indexes(hz());
        vectorCollection.putAsync("1", VectorDocument.of("test1", VectorValues.of(indexName, new float[]{0.5f}, indexName2, new float[]{-0.5f})))
                .toCompletableFuture().join();
        vectorCollection.putAsync("2", VectorDocument.of("test1", VectorValues.of(indexName, new float[]{-0.5f}, indexName2, new float[]{0.5f})))
                .toCompletableFuture().join();

        var results = vectorCollection.searchAsync(VectorValues.of(indexName2, new float[]{0.5f}), SearchOptions.of(
                10, false, true)).toCompletableFuture().join();
        assertEquals(2, results.size());
        // indexName2 has opposite scores than default index
        assertThat(results.results()).toIterable()
                .usingRecursiveFieldByFieldElementComparator(usingOverriddenEqualsIgnoringFields("id"))
                .containsExactly(
                        sr("2", 1f, vec(0.5f)),
                        sr("1", 0f, vec(-0.5f)));
    }

    @Test
    public void testSearch_whenConfiguredWithDim2Vector() {
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(collectionName).addVectorIndexConfig(
                new VectorIndexConfig()
                        .setName(indexName)
                        .setDimension(2)
                        .setMetric(Metric.COSINE)
                        .setUseDeduplication(useDeduplication)
        );
        hz().getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        VectorCollection<String, String> vectorCollection = hz().getVectorCollection(vectorCollectionConfig.getName());
        vectorCollection.putAsync("1", VectorDocument.of("test1", VectorValues.of(new float[] {0.5f, 0.7f})))
                        .toCompletableFuture().join();
        vectorCollection.putAsync("2", VectorDocument.of("test2", VectorValues.of(new float[] {-0.5f, 0.6f})))
                        .toCompletableFuture().join();

        var results = vectorCollection.searchAsync(vec(0.5f, 0.6f), SearchOptions.of(
                10, false, true)).toCompletableFuture().join();

        assertEquals(2, results.size());
        assertThat(results.results()).toIterable()
                .usingRecursiveFieldByFieldElementComparator(usingOverriddenEqualsIgnoringFields("id", "score"))
                .containsExactly(
                        sr("1", 0f, vec(0.5f, 0.7f)),
                        sr("2", 0f, vec(-0.5f, 0.6f)));
    }

    @Test
    public void testSearchReturnsValue() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        vectorCollection.putAsync("1", DOC_1D_POSITIVE).toCompletableFuture().join();
        vectorCollection.putAsync("2", DOC_1D_NEGATIVE).toCompletableFuture().join();

        var results = vectorCollection.searchAsync(vec(0.5f), SearchOptions.of(
                10, true, false)).toCompletableFuture().join();

        assertEquals(2, results.size());
        assertThat(results.results()).toIterable()
                .usingRecursiveFieldByFieldElementComparator(usingOverriddenEqualsIgnoringFields("id"))
                .containsExactly(
                        sr("1", 1f, "positive", null),
                        sr("2", 0.5f, "negative", null));
    }

    @Test
    public void testSearchReturnsValueAndVectors() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        vectorCollection.putAsync("1", DOC_1D_POSITIVE).toCompletableFuture().join();
        vectorCollection.putAsync("2", DOC_1D_NEGATIVE).toCompletableFuture().join();

        var results = vectorCollection.searchAsync(vec(0.5f), SearchOptions.of(
                10, true, true)).toCompletableFuture().join();

        assertEquals(2, results.size());
        assertThat(results.results()).toIterable()
                .usingRecursiveFieldByFieldElementComparator(usingOverriddenEqualsIgnoringFields("id"))
                .containsExactly(
                        sr("1", 1f, "positive", vec(0.5f)),
                        sr("2", 0.5f, "negative", vec(-0.5f)));
    }

    @Test
    public void testSearchAfterDeleteShouldNotReturnDeletedItem() {
        // given
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        vectorCollection.putAsync("1", DOC_1D_POSITIVE)
                .toCompletableFuture().join();
        vectorCollection.putAsync("2", DOC_1D_NEGATIVE)
                .toCompletableFuture().join();

        // when
        assertThat(vectorCollection.deleteAsync("1")).succeedsWithin(TIMEOUT);

        // then
        assertThat(vectorCollection.searchAsync(vec(0.5f), SearchOptions.of(10, false, true)))
                .succeedsWithin(TIMEOUT).satisfies(results -> {
                    assertThat(results.size()).isEqualTo(1);
                    assertThat(toMap(results)).containsOnlyKeys("2");
                });
    }

    @Test
    public void testSearchRespectsPartitionLimitHint() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        // create 2 entries in the same partition and request 2 results, but with partition limit = 1
        // only one of them should be returned
        var key1 = generateKeyForPartition(member, 0);
        var key2 = generateKeyForPartition(member, 0);
        vectorCollection.putAsync(key1, DOC_1D_POSITIVE).toCompletableFuture().join();
        vectorCollection.putAsync(key2, DOC_1D_NEGATIVE).toCompletableFuture().join();

        var results = vectorCollection.searchAsync(vec(0.5f), SearchOptions.builder()
                .limit(2)
                .hint(Hints.PARTITION_LIMIT, 1)
                .build()
        ).toCompletableFuture().join();

        assertEquals(1, results.size());
        assertThat(toMap(results)).containsOnlyKeys(key1);
    }

    @Test
    public void testSearchRespectsMemberLimitHint() {
        assumeThat(clusterSize).as("memberLimit hint is used only on >1 member clusters")
                .isGreaterThan(1);

        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        // create 2 entries in the same partition and request 2 results, but with member limit = 1
        // only one of them should be returned
        var key1 = generateKeyForPartition(member, 0);
        var key2 = generateKeyForPartition(member, 0);
        vectorCollection.putAsync(key1, DOC_1D_POSITIVE).toCompletableFuture().join();
        vectorCollection.putAsync(key2, DOC_1D_NEGATIVE).toCompletableFuture().join();

        var results = vectorCollection.searchAsync(vec(0.5f), SearchOptions.builder()
                .limit(2)
                .hint(Hints.MEMBER_LIMIT, 1)
                .build()
        ).toCompletableFuture().join();

        assertEquals(1, results.size());
        assertThat(toMap(results)).containsOnlyKeys(key1);
    }

    @Test
    public void testSearchRespectsSingleStageHint() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        // create 2 entries in the same partition and request 2 results.
        // with member limit = 1 as the only hint one of them should be returned.
        // with member limit = 1 and singleStage = true both entries should be returned.
        // this is an indirect test of singleStage hint. If the hint is respected, memberLimit will be ignored.
        var key1 = generateKeyForPartition(member, 0);
        var key2 = generateKeyForPartition(member, 0);
        vectorCollection.putAsync(key1, DOC_1D_POSITIVE).toCompletableFuture().join();
        vectorCollection.putAsync(key2, DOC_1D_NEGATIVE).toCompletableFuture().join();

        var results = vectorCollection.searchAsync(vec(0.5f), SearchOptions.builder()
                .limit(2)
                .hint(Hints.MEMBER_LIMIT, 1)
                .hint(Hints.FORCE_SINGLE_STAGE_SEARCH, true)
                .build()
        ).toCompletableFuture().join();

        assertEquals(2, results.size());
        assertThat(toMap(results)).containsOnlyKeys(key1, key2);
    }

    @Test
    public void testSearchValidatesPartitionLimitHint() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        // for the hint validation to happen the collection has to be not empty
        vectorCollection.putAsync("1", DOC_1D_POSITIVE).toCompletableFuture().join();

        assertThat(vectorCollection.searchAsync(vec(0.5f), SearchOptions.builder()
                .limit(2)
                .hint(Hints.PARTITION_LIMIT, -1)
                .build()
        )).failsWithin(TIMEOUT).withThrowableThat().havingRootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .withMessage("Partition limit cannot be negative");

        assertThat(vectorCollection.searchAsync(vec(0.5f), SearchOptions.builder()
                .limit(2)
                .hint(Hints.PARTITION_LIMIT, 3)
                .build()
        )).failsWithin(TIMEOUT).withThrowableThat().havingRootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .withMessage("Number of neighbours requested from partition "
                        + "is greater than requested result size");

        assertThat(vectorCollection.searchAsync(vec(0.5f), SearchOptions.builder()
                .limit(10000)
                .hint(Hints.PARTITION_LIMIT, 1)
                .build()
        )).failsWithin(TIMEOUT).withThrowableThat().havingRootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .withMessage("Number of neighbours requested from partition "
                        + "is not sufficient to generate full requested result");
    }

    @Test
    public void testSearchDoesNotValidateMemberLimitHint() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        // for the hint validation to happen the collection has to be not empty
        vectorCollection.putAsync("1", DOC_1D_POSITIVE).toCompletableFuture().join();

        assertThat(vectorCollection.searchAsync(vec(0.5f), SearchOptions.builder()
                .limit(2)
                .hint(Hints.MEMBER_LIMIT, -1)
                .build()
        )).succeedsWithin(TIMEOUT);

        assertThat(vectorCollection.searchAsync(vec(0.5f), SearchOptions.builder()
                .limit(2)
                .hint(Hints.MEMBER_LIMIT, 3)
                .build()
        )).succeedsWithin(TIMEOUT);

        assertThat(vectorCollection.searchAsync(vec(0.5f), SearchOptions.builder()
                .limit(10000)
                .hint(Hints.MEMBER_LIMIT, 1)
                .build()
        )).succeedsWithin(TIMEOUT);
    }

    @Test
    public void testSearchValidatesEfSearchHint() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        // for the hint validation to happen the collection has to be not empty
        vectorCollection.putAsync("1", DOC_1D_POSITIVE).toCompletableFuture().join();

        assertThat(vectorCollection.searchAsync(vec(0.5f), SearchOptions.builder()
                .limit(10)
                .hint(Hints.EF_SEARCH, -1)
                .build()
        )).failsWithin(TIMEOUT).withThrowableThat().havingRootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .withMessage("efSearch is not sufficient to generate full requested result");

        assertThat(vectorCollection.searchAsync(vec(0.5f), SearchOptions.builder()
                .limit(20)
                .hint(Hints.EF_SEARCH, 1)
                .build()
        )).failsWithin(TIMEOUT).withThrowableThat().havingRootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .withMessage("efSearch is not sufficient to generate full requested result");

        assertThat(vectorCollection.searchAsync(vec(0.5f), SearchOptions.builder()
                .limit(20)
                .hint(Hints.EF_SEARCH, 3)
                .build()
        )).as("Should adjust partitionLimit to efSearch")
                .succeedsWithin(TIMEOUT);

        assertThat(vectorCollection.searchAsync(vec(0.5f), SearchOptions.builder()
                .limit(10)
                .hint(Hints.PARTITION_LIMIT, 5)
                .hint(Hints.EF_SEARCH, 1)
                .build()
        )).failsWithin(TIMEOUT).withThrowableThat().havingRootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .withMessageStartingWith("efSearch (1) is smaller than the number of neighbours requested from partition (5)");

        assertThat(vectorCollection.searchAsync(vec(0.5f), SearchOptions.builder()
                .limit(30)
                .hint(Hints.PARTITION_LIMIT, 2)
                .hint(Hints.EF_SEARCH, 2)
                .build()
        )).as("With explicit hints validation happens later")
                .failsWithin(TIMEOUT).withThrowableThat().havingRootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .withMessageStartingWith("Number of neighbours requested from partition is not sufficient to generate full requested result");

        assertThat(vectorCollection.searchAsync(vec(0.5f), SearchOptions.builder()
                .limit(10)
                .hint(Hints.PARTITION_LIMIT, 5)
                .hint(Hints.EF_SEARCH, 7)
                .build()
        )).as("efSearch must be greater than partitionLimit, not topK")
                .succeedsWithin(TIMEOUT);
    }

    @Test
    public void testOptimize_indexNotExists_then_optimizationFails() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        warmupOneIndexCollection(member, vectorCollection);
        assertThat(vectorCollection.optimizeAsync("invalid_index"))
                .failsWithin(TIMEOUT).withThrowableThat()
                .havingRootCause()
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .withMessage("No index was found with the name: invalid_index");
    }

    @Test
    public void testOptimize_storageNotExists_then_optimizationSuccess() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        assertThat(vectorCollection.optimizeAsync())
                .succeedsWithin(TIMEOUT);
    }

    @Test
    public void testOptimize_storageExists_then_optimizationSuccess() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        warmupOneIndexCollection(member, vectorCollection);

        assertThat(vectorCollection.optimizeAsync()).succeedsWithin(TIMEOUT);
    }

    @Test
    public void testOptimize_interleavedOptimizationSuccess() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        warmupOneIndexCollection(member, vectorCollection);

        for (int i = 0; i < 1000; ++i) {
            assertThat(vectorCollection.putAsync("" + i, VectorDocument.of("value", randomVec(1))))
                    .succeedsWithin(TIMEOUT);
        }

        //when
        // issue 2 concurrent optimizations and some insertions
        var optimizeFuture = vectorCollection.optimizeAsync();
        var optimizeFuture2 = vectorCollection.optimizeAsync(indexName);

        for (int i = 1000; i < 2000; ++i) {
            assertThat(vectorCollection.putAsync("" + i, VectorDocument.of("value", randomVec(1))))
                    .succeedsWithin(TIMEOUT);
        }

        // then
        assertThat(optimizeFuture).succeedsWithin(TIMEOUT);
        assertThat(optimizeFuture2).succeedsWithin(TIMEOUT);

        // ensure that there is nothing preventing the cluster to reach safe state
        waitClusterForSafeState(member);
    }

    @Test
    public void testOptimize_twoIndexStorage_then_noIndexOptimizationFails() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith2Indexes(hz());
        warmupTwoIndexCollection(member, vectorCollection);
        assertThat(vectorCollection.optimizeAsync())
                .failsWithin(TIMEOUT)
                .withThrowableThat()
                .havingRootCause()
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .withMessage("The index name has not been specified.");
    }

    @Test
    public void testOptimize_improveGraphConnectivity() {
        Supplier<String> generateKeyForPartitionZero = () -> generateKeyForPartition(hz(), 0);

        VectorCollection<String, Integer> vectorCollection = getVectorCollectionWith2DimAndLowMaxConnection(hz());
        putSync(vectorCollection, generateKeyForPartitionZero.get(), 1, new float[]{1, 1}); // entry point
        range(100, 150).forEach(i -> putSync(vectorCollection, generateKeyForPartitionZero.get(), i, new float[]{99, i}));
        putSync(vectorCollection, generateKeyForPartitionZero.get(), 2, new float[]{2, 2});
        putSync(vectorCollection, generateKeyForPartitionZero.get(), 3, new float[]{3, 3});

        var vectorSearch = vec(99, 150);
        SearchOptions searchOptions = SearchOptions.builder()
                .limit(100)
                // the distribution is skewed, provide explicit value
                .hint(Hints.PARTITION_LIMIT, 100)
                .build();

        var resultsBeforeOptimization = vectorCollection.searchAsync(vectorSearch, searchOptions).toCompletableFuture().join();

        final int expectedResultsBeforeOptimization = 3;
        assertEquals(expectedResultsBeforeOptimization, resultsBeforeOptimization.size());

        assertThat(vectorCollection.optimizeAsync()).succeedsWithin(TIMEOUT);

        var resultsAfterOptimization = vectorCollection.searchAsync(vectorSearch, searchOptions).toCompletableFuture().join();

        // optimization is non-deterministic and our graph is quite degenerate.
        // sometimes, likely due to unlucky concurrent executions the optimization is unable
        // to fully connect our graph. There seem to be various cases of that resulting
        // in different search result sizes.
        // here we only need to check that optimization actually was invoked.
        assertThat(resultsAfterOptimization.size()).isGreaterThan(expectedResultsBeforeOptimization);
    }

    @Test
    public void testCollectionVisibleInDistributedObjects() {
        assertNotNull(getVectorCollectionWith1Dim(hz()));
        var collection = hz().getVectorCollection(collectionName);
        assertThat(collection).isNotNull();
        // in multi-member cluster distributed object may not be immediately visible
        // on different members than the one used to create it and client may send
        // getDistributedObjects() message to any member.
        assertTrueEventually(() -> assertThat(hz().getDistributedObjects())
                .filteredOn(obj -> collectionName.equals(obj.getName()))
                .singleElement().matches(obj -> VectorCollectionService.SERVICE_NAME.equals(obj.getServiceName())));
    }

    @Test
    public void testVectorCollectionAndIMapAreIndependent() {
        // vector collection created first
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE_NAMED);

        Map<String, String> vcAsMap = hz().getMap(collectionName);
        assertThat(vcAsMap).as("Should not return vector collection entries").isEmpty();
    }

    @Test
    public void testIMapAndVectorCollectionAreIndependent() {
        // IMap created first
        Map<String, String> vcAsMap = hz().getMap(collectionName);
        vcAsMap.put("2", "something");
        vcAsMap.put("3", "something else");

        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        assertThat(vectorCollection.getAsync("2"))
                .as("Should not return IMap entries").succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT).isNull();

        assertThat(vcAsMap).containsOnlyKeys("2", "3");
    }

    @Test
    public void testClear() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.clearAsync()).succeedsWithin(TIMEOUT);
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isNull();
    }

    @Test
    public void testClearAllPartitionsAllIndexes() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith2Indexes(hz());
        var addedEntries = warmupTwoIndexCollection(member, vectorCollection);

        assertThat(vectorCollection.clearAsync()).succeedsWithin(TIMEOUT);

        addedEntries.keySet().forEach(
                key -> assertThat(vectorCollection.getAsync(key))
                        .succeedsWithin(TIMEOUT)
                        .isNull()
        );
    }

    @Test
    public void testDestroyEmptyCollection() {
        var vectorCollection = getVectorCollectionWith1Dim(hz());
        assertTrueEventually(() -> assertThat(hz().getDistributedObjects()).isNotEmpty());

        vectorCollection.destroy();

        assertTrueEventually(() -> assertThat(hz().getDistributedObjects()).isEmpty());
    }

    @Test
    public void testDestroyCollection() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT).isNull();

        vectorCollection.destroy();

        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isNull();
        assertTrueEventually(() -> assertThat(hz().getDistributedObjects()).isEmpty());
    }

    @Test
    public void testDestroyCollectionAllPartitions() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith2Indexes(hz());
        var addedEntries = warmupTwoIndexCollection(member, vectorCollection);

        vectorCollection.destroy();

        addedEntries.keySet().forEach(
                key -> assertThat(vectorCollection.getAsync(key))
                        .succeedsWithin(TIMEOUT)
                        .isNull()
        );
        assertTrueEventually(() -> assertThat(hz().getDistributedObjects()).isEmpty());
    }

    @Test
    public void testSize() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        assertThat(vectorCollection.size()).isEqualTo(0L);
        assertThat(vectorCollection.putAsync("1", DOC_1D_POSITIVE)).succeedsWithin(TIMEOUT).isNull();
        assertThat(vectorCollection.size()).isEqualTo(1L);
    }

    @Test
    public void testSizeAllPartitionExists() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());
        warmupOneIndexCollection(member, vectorCollection);
        var partitionCount = member.getPartitionService().getPartitions().size();
        assertThat(vectorCollection.size()).isEqualTo(partitionCount);
    }

    private <K, T> void putSync(VectorCollection<K, T> vectorCollection, K key, T value, float[] vector) {
        vectorCollection.putAsync(key, VectorDocument.of(value, vec(vector)))
                .toCompletableFuture().join();
    }

    private <K, T> VectorCollection<K, T> getVectorCollectionWith2DimAndLowMaxConnection(HazelcastInstance member) {
        VectorIndexConfig vectorIndexConfig = new VectorIndexConfig()
                .setUseDeduplication(useDeduplication)
                .setMaxDegree(2)
                .setEfConstruction(5)
                .setMetric(Metric.COSINE)
                .setDimension(2);
        if (indexName != null) {
            vectorIndexConfig.setName(indexName);
        }
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(collectionName)
                .addVectorIndexConfig(vectorIndexConfig);
        member.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        return member.getVectorCollection(vectorCollectionConfig.getName());
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

    private VectorCollection<String, String> getVectorCollectionWith2Indexes(HazelcastInstance member) {
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(collectionName)
                .addVectorIndexConfig(new VectorIndexConfig().setName(indexName)
                        .setMetric(Metric.COSINE)
                        .setDimension(1)
                        .setUseDeduplication(useDeduplication))
                .addVectorIndexConfig(new VectorIndexConfig().setName(indexName2)
                        .setMetric(Metric.COSINE)
                        .setDimension(1)
                        .setUseDeduplication(useDeduplication));
        member.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        return member.getVectorCollection(vectorCollectionConfig.getName());
    }
}
