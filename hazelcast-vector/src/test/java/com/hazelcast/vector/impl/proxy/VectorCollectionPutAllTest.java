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

import com.google.common.collect.Lists;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.impl.service.VectorCollectionServiceImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.internal.util.MemoizingSupplier.memoize;
import static com.hazelcast.vector.impl.VectorTestUtils.vec;
import static com.hazelcast.vector.impl.proxy.VectorCollectionProxyTest.DOC_1D_NEGATIVE;
import static com.hazelcast.vector.impl.proxy.VectorCollectionProxyTest.DOC_1D_POSITIVE;
import static com.hazelcast.vector.impl.proxy.VectorCollectionProxyTest.TIMEOUT;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorCollectionPutAllTest extends HazelcastTestSupport {

    private String collectionName = randomName();
    private String indexName = randomName();

    protected TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance[] members;
    private HazelcastInstance member;
    private Supplier<HazelcastInstance> clientSupplier = memoize(() -> factory.newHazelcastClient());

    @Parameterized.Parameters(name = "client={0}, offload={1}")
    public static Object[] parameters() {
        return Lists.cartesianProduct(List.of(List.of(false, true), List.of(0, 1, 10000000)))
                .stream()
                .map(tuple -> tuple.toArray(new Object[0]))
                .toArray(Object[]::new);
    }

    @Parameterized.Parameter
    public boolean useClient;

    @Parameterized.Parameter(1)
    public long offloadMillis;

    @Before
    public void setup() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.setProperty(VectorCollectionServiceImpl.MAX_SUCCESSIVE_OFFLOADED_OP_RUN_MILLIS.getName(), String.valueOf(offloadMillis));
        members = factory.newInstances(config, 2);
        member = members[0];
    }

    @After
    public void shutdownFactory() {
        factory.shutdownAll();
    }

    private HazelcastInstance hz() {
        return useClient ? clientSupplier.get() : member;
    }

    @Test
    public void testPutAllSingleItem() {
        indexName = null;
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putAllAsync(Map.of("1", DOC_1D_POSITIVE))).succeedsWithin(TIMEOUT);
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE);
    }

    @Test
    public void testPutAllMultipleItems() {
        indexName = null;
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putAllAsync(Map.of("1", DOC_1D_POSITIVE, "2", DOC_1D_NEGATIVE))).succeedsWithin(TIMEOUT);
        assertThat(vectorCollection.getAsync("1")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_POSITIVE);
        assertThat(vectorCollection.getAsync("2")).succeedsWithin(TIMEOUT).isEqualTo(DOC_1D_NEGATIVE);
    }

    @Test
    public void testPutAllNoItems() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        assertThat(vectorCollection.putAllAsync(Map.of()))
                .as("Should not fail")
                .succeedsWithin(TIMEOUT);
    }

    @Test
    public void testPutAllOneItemForEachPartition() {
        VectorCollection<String, String> vectorCollection = getVectorCollectionWith1Dim(hz());

        Map<String, VectorDocument<String>> entries =
                IntStream.range(0, hz().getPartitionService().getPartitions().size())
                        .mapToObj(p -> generateKeyForPartition(hz(), "key-", p))
                        .collect(Collectors.toMap(identity(), key -> VectorDocument.of(key, vec(indexName, 1f))));

        assertThat(vectorCollection.putAllAsync(entries))
                .succeedsWithin(TIMEOUT);

        entries.entrySet().forEach(expectedEntry -> assertThat(vectorCollection.getAsync(expectedEntry.getKey()))
                .succeedsWithin(TIMEOUT)
                .isEqualTo(expectedEntry.getValue()));
    }

    @Test
    public void testPutAllManyItems() {
        VectorCollection<Integer, Integer> vectorCollection = getVectorCollectionWith1Dim(hz());

        testPutAllManyItems(vectorCollection);
    }

    @Test
    public void testPutAllManyItemsWithBatching() {
        assumeThat(useClient).as("PutAll batching is not supported on client").isFalse();

        VectorCollection<Integer, Integer> vectorCollection = getVectorCollectionWith1Dim(hz());
        ((VectorCollectionProxy) vectorCollection).setPutAllBatchSize(10);

        testPutAllManyItems(vectorCollection);
    }

    private void testPutAllManyItems(VectorCollection<Integer, Integer> vectorCollection) {
        Map<Integer, VectorDocument<Integer>> entries = new HashMap<>();
        final int entryCount = 10_000;
        for (int i = 0; i < entryCount; ++i) {
            entries.put(i, VectorDocument.of(i + 1, vec(i + 2f)));
        }

        assertThat(vectorCollection.putAllAsync(entries)).succeedsWithin(TIMEOUT);

        for (int i = 0; i < entryCount; ++i) {
            assertThat(vectorCollection.getAsync(i)).succeedsWithin(TIMEOUT)
                    .isEqualTo(VectorDocument.of(i + 1, vec(indexName, i + 2f)));
        }
    }

    private <T> VectorCollection<T, T> getVectorCollectionWith1Dim(HazelcastInstance member) {
        VectorIndexConfig vectorIndexConfig = new VectorIndexConfig()
                .setMetric(Metric.EUCLIDEAN)
                .setDimension(1);
        if (indexName != null) {
            vectorIndexConfig.setName(indexName);
        }
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(collectionName)
                .setBackupCount(offloadMillis > 0 ? 0 : 1)
                .addVectorIndexConfig(vectorIndexConfig);
        member.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        return member.getVectorCollection(vectorCollectionConfig.getName());
    }
}
