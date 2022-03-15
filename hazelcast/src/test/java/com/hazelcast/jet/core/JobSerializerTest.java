/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.cache.ICache;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.collection.IList;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import javax.cache.Cache;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Category({QuickTest.class, ParallelJVMTest.class})
public class JobSerializerTest extends SimpleTestInClusterSupport {

    private static final String SOURCE_MAP_NAME = "source-map";
    private static final String SINK_MAP_NAME = "sink-map";
    private static final String SOURCE_CACHE_NAME = "source-cache";
    private static final String SINK_CACHE_NAME = "sink-cache";
    private static final String SOURCE_LIST_NAME = "source-list";
    private static final String SINK_LIST_NAME = "sink-list";
    private static final String OBSERVABLE_NAME = "observable";

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig();
        config.addCacheConfig(new CacheSimpleConfig().setName(SOURCE_CACHE_NAME))
                .addCacheConfig(new CacheSimpleConfig().setName(SINK_CACHE_NAME));

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig()
                .addSerializerConfig(new SerializerConfig().setTypeClass(Value.class).setClass(ValueSerializer.class));

        initializeWithClient(1, config, clientConfig);
    }

    @Test
    public void when_serializerIsNotRegistered_then_mapThrowsException() {
        Map<Integer, Object> map = instance().getMap(SOURCE_MAP_NAME);

        assertThatThrownBy(() -> map.put(1, new Object())).isInstanceOf(HazelcastSerializationException.class);
    }

    @Test
    public void when_serializerIsRegistered_then_itIsAvailableForLocalMapSource() {
        Map<Integer, Value> map = client().getMap(SOURCE_MAP_NAME);
        map.putAll(ImmutableMap.of(1, new Value(1), 2, new Value(2)));

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.<Integer, Value>map(SOURCE_MAP_NAME))
                .map(entry -> entry.getValue().value())
                .writeTo(AssertionSinks.assertAnyOrder(asList(1, 2)));

        client().getJet().newJob(pipeline, jobConfig()).join();
    }

    @Test
    public void when_serializerIsRegistered_then_itIsAvailableForLocalMapSink() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(1, 2))
                .map(i -> new SimpleEntry<>(i, new Value(i)))
                .writeTo(Sinks.map(SINK_MAP_NAME));

        client().getJet().newJob(pipeline, jobConfig()).join();

        Map<Integer, Value> map = client().getMap(SINK_MAP_NAME);
        assertThat(map).containsExactlyInAnyOrderEntriesOf(
                ImmutableMap.of(1, new Value(1), 2, new Value(2))
        );
    }

    @Test
    public void when_serializerIsNotRegistered_then_cacheThrowsException() {
        Cache<Integer, Object> cache = instance().getCacheManager().getCache(SOURCE_CACHE_NAME);

        assertThatThrownBy(() -> cache.put(1, new Object())).isInstanceOf(HazelcastSerializationException.class);
    }

    @Test
    public void when_serializerIsRegistered_then_itIsAvailableForLocalCacheSource() {
        Cache<Integer, Value> map = client().getCacheManager().getCache(SOURCE_CACHE_NAME);
        map.putAll(ImmutableMap.of(1, new Value(1), 2, new Value(2)));

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.<Integer, Value>cache(SOURCE_CACHE_NAME))
                .map(entry -> entry.getValue().value())
                .writeTo(AssertionSinks.assertAnyOrder(asList(1, 2)));

        client().getJet().newJob(pipeline, jobConfig()).join();
    }

    @Test
    public void when_serializerIsRegistered_then_itIsAvailableForLocalCacheSink() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(1, 2))
                .map(i -> new SimpleEntry<>(i, new Value(i)))
                .writeTo(Sinks.cache(SINK_CACHE_NAME));

        client().getJet().newJob(pipeline, jobConfig()).join();

        ICache<Integer, Value> cache = client().getCacheManager().getCache(SINK_CACHE_NAME);
        assertThat(cache).hasSize(2);
        assertThat(cache.getAll(ImmutableSet.of(1, 2))).containsExactlyInAnyOrderEntriesOf(
                ImmutableMap.of(1, new Value(1), 2, new Value(2))
        );
    }

    @Test
    public void when_serializerIsNotRegistered_then_listThrowsException() {
        List list = instance().getList(SOURCE_MAP_NAME);

        assertThatThrownBy(() -> list.add(new Object())).isInstanceOf(HazelcastSerializationException.class);
    }

    @Test
    public void when_serializerIsRegistered_then_itIsAvailableForLocalListSource() {
        List<Value> list = client().getList(SOURCE_LIST_NAME);
        list.addAll(asList(new Value(1), new Value(2)));

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.<Value>list(SOURCE_LIST_NAME))
                .map(Value::value)
                .writeTo(AssertionSinks.assertAnyOrder(asList(1, 2)));

        client().getJet().newJob(pipeline, jobConfig()).join();
    }

    @Test
    public void when_serializerIsRegistered_then_itIsAvailableForLocalListSink() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(1, 2))
                .map(Value::new)
                .writeTo(Sinks.list(SINK_LIST_NAME));

        client().getJet().newJob(pipeline, jobConfig()).join();

        IList<Value> list = client().getList(SINK_LIST_NAME);
        assertThat(list).containsExactlyInAnyOrder(
                new Value(1), new Value(2)
        );
    }

    @Test
    public void when_serializerIsRegistered_then_itIsAvailableForLocalObservableSink() throws Exception {
        // Given
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(1, 2))
                .map(Value::new)
                .writeTo(Sinks.observable(OBSERVABLE_NAME));

        // When
        Observable<Value> observable = client().getJet().getObservable(OBSERVABLE_NAME);
        CompletableFuture<Long> counter = observable.toFuture(values -> values.map(Value::value).count());

        // Then
        client().getJet().newJob(pipeline, jobConfig()).join();
        assertThat(counter.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS).intValue()).isEqualTo(2);
    }

    private static JobConfig jobConfig() {
        return new JobConfig().registerSerializer(Value.class, ValueSerializer.class);
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
