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

package com.hazelcast.vector.jet;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static com.hazelcast.vector.impl.VectorTestUtils.vec;
import static com.hazelcast.vector.impl.proxy.VectorCollectionProxyTest.TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
public class VectorSinkTest extends HazelcastTestSupport {
    private static final int ENTRY_COUNT = 100;

    private final String collectionName = randomName();
    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance[] instances;
    private VectorCollection<Integer, String> collection;

    @Parameterized.Parameters(name = "useCollection={0} useDocumentFunction={1}")
    public static List<Object[]> parameters() {
        return cartesianProduct(List.of(true, false), List.of(true, false));
    }

    @Parameterized.Parameter
    public boolean useCollection;

    @Parameterized.Parameter(1)
    public boolean useDocumentFunction;

    @Before
    public void setup() {
        instances = factory.newInstances(smallInstanceConfig(), 2);
        collection = getVectorCollectionWith1Dim(instances[0]);
    }

    @After
    public void shutdownFactory() {
        factory.shutdownAll();
    }

    @Test
    public void batchPipelineShouldWriteData() {
        var p = Pipeline.create();
        p.readFrom(TestSources.itemsDistributed(List.of(IntStream.range(0, ENTRY_COUNT).boxed().toArray(Integer[]::new))))
            .writeTo(getSink());

        var job = instances[0].getJet().newJob(p);
        job.join();

        for (int i = 0; i < ENTRY_COUNT; ++i) {
            assertThat(collection.getAsync(i)).succeedsWithin(TIMEOUT)
                    .isEqualTo(VectorDocument.of("" + i, vec(i * 0.1f)));
        }
    }

    @Test
    public void streamPipelineShouldWriteDataEventually() {
        var p = Pipeline.create();
        p.readFrom(TestSources.itemStream(ENTRY_COUNT)).withoutTimestamps()
                .map(se -> (int) se.sequence())
                .writeTo(getSink());

        var job = instances[0].getJet().newJob(p);

        assertTrueEventually(() -> {
            for (int i = 0; i < ENTRY_COUNT; ++i) {
                assertThat(collection.getAsync(i)).succeedsWithin(TIMEOUT)
                        .isEqualTo(VectorDocument.of("" + i, vec(i * 0.1f)));
            }
        });

        job.cancel();
    }

    @Test
    public void jobWithCustomSerializersShouldWriteData() {
        var p = Pipeline.create();
        p.readFrom(TestSources.itemsDistributed(List.of(IntStream.range(0, ENTRY_COUNT).boxed().toArray(Integer[]::new))))
                .map(Value::new)
                .writeTo(getSink(Value::value));

        var job = instances[0].getJet().newJob(p, new JobConfig()
                .registerSerializer(Value.class, ValueSerializer.class));
        job.join();

        // Custom serializer is needed to check the collection content.
        // It should not be added to member config, so we can check that the serializer
        // configured in the job is used (as opposed to globally configured serializers).
        // Client needs the custom serializer to correctly invoke `getAsync`.
        // Such configuration provides sufficient isolation for the serializers.
        // See also: JobSerializerTest
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig()
                .addSerializerConfig(new SerializerConfig().setTypeClass(Value.class).setClass(ValueSerializer.class));
        var client = factory.newHazelcastClient(clientConfig);
        VectorCollection<Value, String> clientCollection = client.getVectorCollection(collectionName);

        for (int i = 0; i < ENTRY_COUNT; ++i) {
            assertThat(clientCollection.getAsync(new Value(i))).succeedsWithin(TIMEOUT)
                    .isEqualTo(VectorDocument.of("value=" + i, vec(i * 0.1f)));
        }
    }

    private Sink<Integer> getSink() {
        return getSink(FunctionEx.identity());
    }

    private <T> Sink<T> getSink(FunctionEx<T, Integer> converter) {
        // test all sink method variants
        if (useCollection) {
            if (useDocumentFunction) {
                return VectorSinks.vectorCollection((VectorCollection<T, String>) collection,
                        FunctionEx.identity(),
                        String::valueOf,
                        i -> vec(converter.apply(i) * 0.1f));
            } else {
                return VectorSinks.vectorCollection((VectorCollection<T, String>) collection,
                        FunctionEx.identity(),
                        i -> VectorDocument.of(i.toString(), vec(converter.apply(i) * 0.1f)));
            }
        } else {
            if (useDocumentFunction) {
                return VectorSinks.vectorCollection(collectionName,
                        FunctionEx.identity(),
                        String::valueOf,
                        i -> vec(converter.apply(i) * 0.1f));
            } else {
                return VectorSinks.vectorCollection(collectionName,
                        FunctionEx.identity(),
                        i -> VectorDocument.of(i.toString(), vec(converter.apply(i) * 0.1f)));
            }
        }
    }

    private <K, V> VectorCollection<K, V> getVectorCollectionWith1Dim(HazelcastInstance member) {
        VectorIndexConfig vectorIndexConfig = new VectorIndexConfig()
                .setMetric(Metric.EUCLIDEAN)
                .setDimension(1);
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(collectionName)
                .addVectorIndexConfig(vectorIndexConfig);
        member.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        return member.getVectorCollection(vectorCollectionConfig.getName());
    }

    private static final class Value {

        private final int value;

        private Value(int value) {
            this.value = value;
        }

        public int value() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Value value1 = (Value) o;
            return value == value1.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public String toString() {
            return "value=" + value;
        }
    }

    private static class ValueSerializer implements StreamSerializer<Value> {

        @Override
        public int getTypeId() {
            return 1;
        }

        @Override
        public void write(ObjectDataOutput output, Value value) throws IOException {
            output.writeInt(value.value);
        }

        @Override
        @Nonnull
        public Value read(ObjectDataInput input) throws IOException {
            return new Value(input.readInt());
        }
    }
}
