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

package com.hazelcast.vector.internal.impl.metrics;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.impl.CapturingCollector;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.vector.VectorCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.VECTOR_COLLECTION_PREFIX;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class})
public abstract class VectorCollectionMetricsTestBase extends HazelcastTestSupport {

    protected final String collectionName = randomName();
    private String indexName = "index1";

    protected TestHazelcastFactory factory = new TestHazelcastFactory();
    protected HazelcastInstance member;
    private HazelcastInstance liteMember;
    private HazelcastInstance client;

    public enum OperationSource { MEMBER, LITE_MEMBER, CLIENT }

    @Parameterized.Parameters(name = "operationSource={0}")
    public static List<Object[]> parameters() {
        return cartesianProduct(List.of(OperationSource.values()));
    }

    @Parameterized.Parameter
    public OperationSource operationSource;

    @Before
    public void setup() {
        var members = factory.newInstances(getConfig(), getClusterSize());
        member = members[0];
        if (operationSource == OperationSource.LITE_MEMBER) {
            liteMember = factory.newHazelcastInstance(getConfig().setLiteMember(true));
        }
        client = factory.newHazelcastClient();
    }

    @Override
    @Nonnull
    protected Config getConfig() {
        return smallInstanceConfigWithoutJetAndMetrics();
    }

    protected int getClusterSize() {
        return 1;
    }

    @After
    public void shutdownFactory() {
        factory.shutdownAll();
    }

    protected HazelcastInstance operationInstance() {
        return switch (operationSource) {
            case MEMBER -> member;
            case LITE_MEMBER -> liteMember;
            case CLIENT -> client;
        };
    }

    protected HazelcastInstance statsInstance() {
        // lite member has its own stats
        return operationSource == OperationSource.LITE_MEMBER ? liteMember : member;
    }

    protected static void assertStats(HazelcastInstance hz, Consumer<Map<MetricDescriptor, CapturingCollector.Capture>> assertFn) {
        MetricsRegistry metricsRegistry = getNodeEngineImpl(hz).getMetricsRegistry();
        CapturingCollector collector = new CapturingCollector();
        metricsRegistry.collect(collector);

        // filter metrics to get more readable diagnostics
        var captures = collector.captures().entrySet().stream()
                .filter(e -> VECTOR_COLLECTION_PREFIX.equals(e.getKey().prefix()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertFn.accept(captures);
    }

    protected static void assertMetricEqual(Map<MetricDescriptor, CapturingCollector.Capture> captures, String name,
                                          long expectedValue) {
        assertMetric(captures, name, value -> assertThat(value).as("Wrong value for metric %s", name).isEqualTo(expectedValue));
    }

    protected static void assertMetric(Map<MetricDescriptor, CapturingCollector.Capture> captures, String name,
                                     Consumer<Long> assertion) {
        var capture = captures.entrySet().stream().filter(e -> e.getKey().metric().equals(name)).findFirst();
        assertThat(capture).as("Captures %s do not contain metric %s", captures, name).isPresent();
        assertion.accept(capture.get().getValue().singleCapturedValue().longValue());
    }

    protected <T> VectorCollection<T, T> getVectorCollectionWith1Dim(HazelcastInstance member) {
        VectorIndexConfig vectorIndexConfig = new VectorIndexConfig()
                .setMetric(Metric.EUCLIDEAN)
                .setDimension(1);
        if (indexName != null) {
            vectorIndexConfig.setName(indexName);
        }
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(collectionName)
                .addVectorIndexConfig(vectorIndexConfig);
        member.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        return member.getVectorCollection(vectorCollectionConfig.getName());
    }
}
