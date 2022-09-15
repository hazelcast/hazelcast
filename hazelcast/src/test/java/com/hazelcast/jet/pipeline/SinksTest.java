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

package com.hazelcast.jet.pipeline;

import com.hazelcast.cache.ICache;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.core.test.TestProcessorSupplierContext;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.pipeline.AbstractStage.transformOf;
import static com.hazelcast.jet.json.JsonUtil.hazelcastJsonValue;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class})
public class SinksTest extends PipelineTestSupport {

    private static HazelcastInstance remoteHz;
    private static ClientConfig clientConfig;

    @BeforeClass
    public static void setUp() {
        Config config = new Config();
        config.setClusterName(randomName());
        config.addCacheConfig(new CacheSimpleConfig().setName("*"));
        remoteHz = createRemoteCluster(config, 2).get(0);
        clientConfig = getClientConfigForRemoteCluster(remoteHz);
    }

    @AfterClass
    public static void afterClass() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void when_setName_then_sinkHasIt() {
        //Given
        String sinkName = randomName();
        String stageName = randomName();
        SinkStage stage = p
                .readFrom(Sources.list(sinkName))
                .writeTo(Sinks.list(sinkName));

        //When
        stage = stage.setName(stageName);

        //Then
        assertEquals(stageName, stage.name());
    }

    @Test
    public void when_setLocalParallelism_then_sinkHasIt() {
        //Given
        String sinkName = randomName();
        int localParallelism = 5;
        SinkStage stage = p
                .readFrom(Sources.list(sinkName))
                .writeTo(Sinks.list(sinkName));

        //When
        stage.setLocalParallelism(localParallelism);

        //Then
        assertEquals(localParallelism, transformOf(stage).localParallelism());
    }

    @Test
    public void when_writeToMultipleStagesToSingleSink_then_allItemsInSink() {
        // Given
        String secondSourceName = randomName();
        List<Integer> input = sequence(itemCount);
        addToSrcList(input);
        hz().getList(secondSourceName).addAll(input);
        BatchStage<Entry<Object, Object>> firstSource = p.readFrom(Sources.list(srcName));
        BatchStage<Entry<Object, Object>> secondSource = p.readFrom(Sources.list(secondSourceName));

        // When
        p.writeTo(Sinks.list(sinkName), firstSource, secondSource);

        // Then
        execute();
        assertEquals(itemCount * 2, sinkList.size());
    }

    @Test
    public void cache_byName() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcCache(input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.cache(sinkName);

        // Then
        p.readFrom(Sources.<String, Integer>cache(srcName)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        ICache<String, Integer> cache = hz().getCacheManager().getCache(sinkName);
        assertEquals(expected.size(), cache.size());
        expected.forEach(entry -> assertEquals(entry.getValue(), cache.get(entry.getKey())));
    }

    @Test
    public void remoteCache() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcCache(input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteCache(sinkName, clientConfig);

        // Then
        p.readFrom(Sources.<String, Integer>cache(srcName)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        ICache<String, Integer> remoteCache = remoteHz.getCacheManager().getCache(sinkName);
        assertEquals(expected.size(), remoteCache.size());
        expected.forEach(entry -> assertEquals(entry.getValue(), remoteCache.get(entry.getKey())));
    }

    @Test
    public void map_byName() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.map(sinkName);

        // Then
        p.readFrom(Sources.<String, Integer>map(srcName)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = hz().<String, Integer>getMap(sinkName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void map_byRef() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        IMap<String, Integer> sinkMap = hz().getMap(sinkName);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.map(sinkMap);

        // Then
        p.readFrom(Sources.<String, Integer>map(srcName)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = sinkMap.entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void map_withToKeyValueFunctions() {
        // Given
        BatchStage<Integer> sourceStage = p.readFrom(TestSources.items(0, 1, 2, 3, 4));

        // When
        sourceStage.writeTo(Sinks.map(sinkName, t -> t, Object::toString));

        // Then
        execute();
        IMap<Integer, String> sinkMap = hz().getMap(sinkName);
        assertEquals(5, sinkMap.size());
        IntStream.range(0, 5).forEach(i -> assertEquals(String.valueOf(i), sinkMap.get(i)));
    }

    @Test
    public void map_withJsonKeyValue() {
        // Given
        BatchStage<Integer> sourceStage = p.readFrom(TestSources.items(0, 1, 2, 3, 4));

        // When
        sourceStage.writeTo(Sinks.map(sinkName, JsonUtil::hazelcastJsonValue, JsonUtil::hazelcastJsonValue));

        // Then
        execute();
        IMap<HazelcastJsonValue, HazelcastJsonValue> sinkMap = hz().getMap(sinkName);
        assertEquals(5, sinkMap.size());
        IntStream.range(0, 5).forEach(i -> assertEquals(hazelcastJsonValue(String.valueOf(i)),
                sinkMap.get(hazelcastJsonValue(i))));
    }

    @Test
    public void remoteMap() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteMap(sinkName, clientConfig);

        // Then
        p.readFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = remoteHz.<String, Integer>getMap(sinkName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithMerging_byName() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithMerging(
                srcName,
                Entry::getKey,
                Entry::getValue,
                // intentionally not a method reference - https://bugs.openjdk.java.net/browse/JDK-8154236
                (oldValue, newValue) -> oldValue + newValue);

        // Then
        p.readFrom(Sources.<String, Integer>map(srcName)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + i))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = hz().<String, Integer>getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithMerging_byRef() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        IMap<String, Integer> srcMap = hz().getMap(srcName);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithMerging(
                srcMap,
                Entry::getKey,
                Entry::getValue,
                // intentionally not a method reference - https://bugs.openjdk.java.net/browse/JDK-8154236
                (oldValue, newValue) -> oldValue + newValue);

        // Then
        p.readFrom(Sources.<String, Integer>map(srcName)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + i))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = hz().<String, Integer>getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithMerging2_byRef() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        IMap<String, Integer> srcMap = hz().getMap(srcName);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithMerging(
                srcMap,
                // intentionally not a method reference - https://bugs.openjdk.java.net/browse/JDK-8154236
                (oldValue, newValue) -> oldValue + newValue);

        // Then
        p.readFrom(Sources.<String, Integer>map(srcName)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + i))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = hz().<String, Integer>getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithMerging_when_functionReturnsNull_then_keyIsRemoved() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithMerging(srcName, (oldValue, newValue) -> null);

        // Then
        p.readFrom(Sources.<String, Integer>map(srcName)).writeTo(sink);
        execute();
        Set<Entry<String, Integer>> actual = hz().<String, Integer>getMap(srcName).entrySet();
        assertEquals(0, actual.size());
    }

    @Test
    public void mapWithMerging_when_entryIsLocked_then_entryIsUpdatedRegardlessTheLock() {
        // Given
        srcMap.put("key", 1);
        srcMap.lock("key");

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithMerging(srcName, (oldValue, newValue) -> oldValue + 1);

        // Then
        p.readFrom(Sources.<String, Integer>map(srcName)).writeTo(sink);
        execute();
        assertEquals(1, srcMap.size());
        assertEquals(2, srcMap.get("key").intValue());
    }

    @Test
    public void mapWithMerging_when_sameKey_then_valuesMerged() {
        // Given
        List<Integer> input = sequence(itemCount);
        hz().getList(srcName).addAll(input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithMerging(
                srcName,
                // intentionally not a method reference - https://bugs.openjdk.java.net/browse/JDK-8154236
                (oldValue, newValue) -> oldValue + newValue);

        // Then
        p.readFrom(Sources.<Integer>list(srcName))
         .map(e -> entry("listSum", e))
         .writeTo(sink);
        execute();
        IMap<Object, Object> actual = hz().getMap(srcName);
        assertEquals(1, actual.size());
        assertEquals(((itemCount - 1) * itemCount) / 2, actual.get("listSum"));
    }

    @Test
    public void mapWithMerging_when_multipleValuesForSingleKeyInABatch() throws Exception {
        ProcessorMetaSupplier metaSupplier = adaptSupplier(SinkProcessors.<Entry<String, Integer>, String,
                Integer>mergeMapP(sinkName, Entry::getKey, Entry::getValue, Integer::sum));

        TestProcessorSupplierContext psContext = new TestProcessorSupplierContext().setHazelcastInstance(member);
        Processor p = TestSupport.supplierFrom(metaSupplier, psContext).get();

        TestOutbox outbox = new TestOutbox();
        p.init(outbox, new TestProcessorContext().setHazelcastInstance(member));
        TestInbox inbox = new TestInbox();
        inbox.add(entry("k", 1));
        inbox.add(entry("k", 2));
        p.process(0, inbox);
        assertTrue("inbox.isEmpty()", inbox.isEmpty());
        assertTrueEventually(() -> {
            assertTrue("p.complete()", p.complete());
        }, 10);
        p.close();

        // assert the output map contents
        IMap<Object, Object> actual = member.getMap(sinkName);
        assertEquals(1, actual.size());
        assertEquals(3, actual.get("k"));
    }

    @Test
    public void mapWithMerging_when_targetHasPartitionStrategy() {
        String targetMap = randomMapName();
        member.getConfig().addMapConfig(new MapConfig(targetMap)
                .setPartitioningStrategyConfig(
                        new PartitioningStrategyConfig(StringPartitioningStrategy.class.getName())));

        List<Integer> input = sequence(itemCount);
        hz().getList(srcName).addAll(input);

        p.readFrom(Sources.<Integer>list(srcName))
         .map(e -> {
             e = e % 100;
             return entry(e + "@" + e, e);
         })
         .writeTo(Sinks.mapWithMerging(targetMap, Integer::sum));
        execute();
        Map<String, Integer> actual = new HashMap<>(hz().getMap(targetMap));
        Map<String, Integer> expected =
                input.stream()
                     .map(e -> {
                         e = e % 100;
                         return entry(e + "@" + e, e);
                     })
                     .collect(toMap(Entry::getKey, Entry::getValue, Integer::sum));
        assertEquals(expected, actual);
    }

    @Test
    public void remoteMapWithMerging() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteMapWithMerging(
                srcName,
                clientConfig,
                Entry::getKey,
                Entry::getValue,
                // intentionally not a method reference - https://bugs.openjdk.java.net/browse/JDK-8154236
                (oldValue, newValue) -> oldValue + newValue);

        // Then
        p.readFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + i))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = remoteHz.<String, Integer>getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void remoteMapWithMerging_when_functionReturnsNull_then_keyIsRemoved() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);
        BatchSource<Entry<String, Integer>> source = Sources.remoteMap(srcName, clientConfig);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteMapWithMerging(
                srcName, clientConfig, (oldValue, newValue) -> null);

        // Then
        p.readFrom(source).writeTo(sink);
        execute();
        Set<Entry<String, Integer>> actual = remoteHz.<String, Integer>getMap(srcName).entrySet();
        assertEquals(0, actual.size());
    }

    @Test
    public void mapWithUpdating_byName() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithUpdating(
                srcName,
                Entry::getKey,
                (Integer value, Entry<String, Integer> item) -> value + 10);

        // Then
        SinkStage sinkStage = p.readFrom(Sources.<String, Integer>map(srcName)).writeTo(sink);
        sinkStage.setLocalParallelism(2);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + 10))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = hz().<String, Integer>getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithUpdating_byRef() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        IMap<String, Integer> srcMap = hz().getMap(srcName);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithUpdating(
                srcMap,
                Entry::getKey,
                (Integer value, Entry<String, Integer> item) -> value + 10);

        // Then
        SinkStage sinkStage = p.readFrom(Sources.<String, Integer>map(srcName)).writeTo(sink);
        sinkStage.setLocalParallelism(2);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + 10))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = srcMap.entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithUpdating_withKeyFn_byRef() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        IMap<String, Integer> srcMap = hz().getMap(srcName);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithUpdating(
                srcMap,
                (Integer value, Entry<String, Integer> item) -> value + 10);

        // Then
        SinkStage sinkStage = p.readFrom(Sources.<String, Integer>map(srcName)).writeTo(sink);
        sinkStage.setLocalParallelism(2);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + 10))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = srcMap.entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithUpdating_when_functionReturnsNull_then_keyIsRemoved() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithUpdating(srcName, (value, item) -> null);

        // Then
        p.readFrom(Sources.<String, Integer>map(srcName)).writeTo(sink);
        execute();
        Set<Entry<String, Integer>> actual = hz().<String, Integer>getMap(srcName).entrySet();
        assertEquals(0, actual.size());
    }

    @Test
    public void mapWithUpdating_when_itemDataSerializable_then_exceptionShouldNotThrown() {
        // Given
        IMap<String, DataSerializableObject> sourceMap = hz().getMap(srcName);
        List<Integer> input = sequence(itemCount);
        input.forEach(i -> sourceMap.put(String.valueOf(i), new DataSerializableObject(i)));

        // When
        Sink<Entry<String, DataSerializableObject>> sink = Sinks.mapWithUpdating(srcName,
                (value, item) -> new DataSerializableObject(value.value + item.getValue().value));

        // Then
        p.readFrom(Sources.<String, DataSerializableObject>map(srcName)).writeTo(sink);
        execute();
        List<Entry<String, DataSerializableObject>> expected = input
                .stream()
                .map(i -> entry(String.valueOf(i), new DataSerializableObject(i * 2)))
                .collect(toList());
        Set<Entry<String, DataSerializableObject>> actual = sourceMap.entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithUpdating_when_entryIsLocked_then_entryIsUpdatedRegardlessTheLock() {
        // Given
        srcMap.put("key", 1);
        srcMap.lock("key");

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithUpdating(srcName, (value, item) -> 2);

        // Then
        p.readFrom(Sources.<String, Integer>map(srcName)).writeTo(sink);
        execute();
        assertEquals(1, srcMap.size());
        assertEquals(2, srcMap.get("key").intValue());
    }

    @Test
    public void remoteMapWithUpdating() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteMapWithUpdating(
                srcName,
                clientConfig,
                Entry::getKey,
                (Integer value, Entry<String, Integer> item) -> value + 10);

        // Then
        p.readFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + 10))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = remoteHz.<String, Integer>getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void remoteMapWithUpdating_when_functionReturnsNull_then_keyIsRemoved() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteMapWithUpdating(srcName, clientConfig, (value, item) -> null);

        // Then
        p.readFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        Set<Entry<Object, Object>> actual = remoteHz.getMap(srcName).entrySet();
        assertEquals(0, actual.size());
    }

    @Test
    public void remoteMapWithUpdating_when_itemDataSerializable() {
        // Given
        IMap<String, DataSerializableObject> sourceMap = remoteHz.getMap(srcName);
        List<Integer> input = sequence(itemCount);
        input.forEach(i -> sourceMap.put(String.valueOf(i), new DataSerializableObject(i)));

        // When
        Sink<Entry<String, DataSerializableObject>> sink = Sinks.remoteMapWithUpdating(
                srcName,
                clientConfig,
                (value, item) -> new DataSerializableObject(value.value + item.getValue().value));

        // Then
        p.readFrom(Sources.<String, DataSerializableObject>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        List<Entry<String, DataSerializableObject>> expected = input
                .stream()
                .map(i -> entry(String.valueOf(i), new DataSerializableObject(i * 2)))
                .collect(toList());
        Set<Entry<String, DataSerializableObject>> actual = sourceMap.entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithEntryProcessor_byName() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithEntryProcessor(
                srcName, Entry::getKey, entry -> new IncrementEntryProcessor<>(10));

        // Then
        p.readFrom(Sources.<String, Integer>map(srcName)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + 10))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = hz().<String, Integer>getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithEntryProcessor_byRef() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        IMap<String, Integer> map = hz().getMap(srcName);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithEntryProcessor(
                map, Entry::getKey, entry -> new IncrementEntryProcessor<>(10));

        // Then
        p.readFrom(Sources.<String, Integer>map(srcName)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + 10))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = map.entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void remoteMapWithEntryProcessor() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteMapWithEntryProcessor(
                srcName,
                clientConfig,
                Entry::getKey,
                entry -> new IncrementEntryProcessor<>(10));

        // Then
        p.readFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + 10))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = remoteHz.<String, Integer>getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithEntryProcessor_when_entryIsLocked_then_entryIsNotUpdated() {
        // Given
        srcMap.put("key", 1);
        srcMap.lock("key");

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithEntryProcessor(
                srcName,
                Entry::getKey,
                entry -> new IncrementEntryProcessor<>(10));

        // Then
        p.readFrom(Sources.<String, Integer>map(srcName)).writeTo(sink);
        Job job = hz().getJet().newJob(p);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        assertEquals(1, srcMap.size());
        assertEquals(1, srcMap.get("key").intValue());
        srcMap.unlock("key");
        assertTrueEventually(() -> assertEquals(11, srcMap.get("key").intValue()), 10);
        job.join();
    }

    @Test
    @Category(SlowTest.class)
    public void mapWithEntryProcessor_testBackpressure() {
        /*
        NOTE TO THE TEST
        This test tries to test that when a permit for async op is denied, the
        processor correctly yields. We don't assert that it actually happened,
        nor that the backpressure was applied to the upstream. We only try to
        simulate a slow sink (using the SleepingEntryProcessor) and check that
        the results are correct.
         */
        String targetMap = randomMapName();

        List<Integer> input = sequence(5_001);
        p.readFrom(TestSources.items(input))
         .writeTo(Sinks.mapWithEntryProcessor(targetMap, FunctionEx.identity(), SleepingEntryProcessor::new));
        execute();
        Map<Integer, Integer> actual = new HashMap<>(hz().getMap(targetMap));
        Map<Integer, Integer> expected =
                input.stream()
                     .collect(toMap(Function.identity(), Function.identity(), Integer::sum));
        assertEquals(expected, actual);
    }

    @Test(expected = IllegalStateException.class)
    public void when_usedTwice_then_throwException() {
        // Given
        BatchStage<Entry<Object, Object>> stage1 = p.readFrom(Sources.map(srcName));
        BatchStage<Entry<Object, Object>> stage2 = p.readFrom(Sources.map(srcName + '2'));
        Sink<Object> sink = Sinks.list(sinkName);
        stage1.writeTo(sink);

        // When
        stage2.writeTo(sink);

        // Then IllegalStateException thrown
    }

    @Test
    public void list_byName() {
        // Given
        populateList(srcList);

        // When
        Sink<Object> sink = Sinks.list(sinkName);

        // Then
        p.readFrom(Sources.list(srcList)).writeTo(sink);
        execute();
        assertEquals(itemCount, sinkList.size());
    }

    @Test
    public void list_byRef() {
        // Given
        populateList(srcList);

        // When
        Sink<Object> sink = Sinks.list(sinkList);

        // Then
        p.readFrom(Sources.list(srcList)).writeTo(sink);
        execute();
        assertEquals(itemCount, sinkList.size());
    }

    @Test
    public void remoteList() {
        // Given
        populateList(srcList);

        // When
        Sink<Object> sink = Sinks.remoteList(sinkName, clientConfig);

        // Then
        p.readFrom(Sources.list(srcName)).writeTo(sink);
        execute();
        assertEquals(itemCount, remoteHz.getList(sinkName).size());
    }

    @Test
    public void reliableTopic_byName() {
        // Given
        populateList(srcList);

        List<Object> receivedList = new ArrayList<>();
        hz().getReliableTopic(sinkName).addMessageListener(message -> receivedList.add(message.getMessageObject()));

        // When
        Sink<Object> sink = Sinks.reliableTopic(sinkName);
        p.readFrom(Sources.list(srcName)).writeTo(sink);
        execute();

        // Then
        assertTrueEventually(() -> assertEquals(itemCount, receivedList.size()));
    }

    @Test
    public void reliableTopic_byRef() {
        // Given
        populateList(srcList);

        List<Object> receivedList = new ArrayList<>();
        hz().getReliableTopic(sinkName).addMessageListener(message -> receivedList.add(message.getMessageObject()));

        // When
        Sink<Object> sink = Sinks.reliableTopic(hz().getReliableTopic(sinkName));
        p.readFrom(Sources.list(srcName)).writeTo(sink);
        execute();

        // Then
        assertTrueEventually(() -> assertEquals(itemCount, receivedList.size()));
    }

    @Test
    public void remoteReliableTopic() {
        // Given
        populateList(srcList);

        List<Object> receivedList = new ArrayList<>();
        remoteHz.getReliableTopic(sinkName).addMessageListener(message -> receivedList.add(message.getMessageObject()));

        // When
        Sink<Object> sink = Sinks.remoteReliableTopic(sinkName, clientConfig);
        p.readFrom(Sources.list(srcName)).writeTo(sink);
        execute();

        // Then
        assertTrueEventually(() -> assertEquals(itemCount, receivedList.size()));
    }

    @Test
    public void remoteReliableTopicSinkClosesClient() {
        // Check we are in a clean state
        assertThat(HazelcastClient.getAllHazelcastClients()).hasSize(1);

        // When
        Sink<Object> sink = Sinks.remoteReliableTopic(sinkName, clientConfig);
        p.readFrom(Sources.list(srcName)).writeTo(sink);
        execute();

        assertThat(HazelcastClient.getAllHazelcastClients()).hasSize(1);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_adaptingPartitionFunction() {
        Pipeline p = Pipeline.create();
        StreamStage<KeyedWindowResult<String, Long>> input1 =
                p.readFrom(TestSources.items(0))
                 .addTimestamps(i -> i, 0)
                 .groupingKey(item -> "key0")
                 .window(WindowDefinition.sliding(1, 1))
                 .aggregate(AggregateOperations.counting());
        BatchStage<Entry<String, Long>> input2 =
                p.readFrom(TestSources.items(entry("key1", 2L)));

        IMap<String, Long> sinkMap = hz().getMap(randomMapName());
        p.writeTo(Sinks.map(sinkMap), input1, input2);

        hz().getJet().newJob(p).join();
        assertEquals(2, sinkMap.size());
        assertEquals((Long) 1L, sinkMap.get("key0"));
        assertEquals((Long) 2L, sinkMap.get("key1"));
    }

    @Test
    public void noop() {
        // Given
        populateList(srcList);

        // When
        Sink<Object> sink = Sinks.noop();

        // Then
        p.readFrom(Sources.list(srcName)).writeTo(sink);
        execute();
        // works without error
    }

    private void populateList(List<Object> list) {
        list.addAll(sequence(itemCount));
    }

    private static class IncrementEntryProcessor<K> implements EntryProcessor<K, Integer, Void> {

        private Integer value;

        IncrementEntryProcessor(Integer value) {
            this.value = value;
        }

        @Override
        public Void process(Entry<K, Integer> entry) {
            entry.setValue(entry.getValue() == null ? value : entry.getValue() + value);
            return null;
        }
    }

    private static class DataSerializableObject implements DataSerializable {
        int value;

        DataSerializableObject() {
        }

        DataSerializableObject(int value) {
            this.value = value;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            value = in.readInt();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DataSerializableObject that = (DataSerializableObject) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return value;
        }
    }

    private static final class SleepingEntryProcessor implements EntryProcessor<Integer, Object, Void> {
        private final int v;

        private SleepingEntryProcessor(int v) {
            this.v = v;
        }

        @Override
        public Void process(Entry<Integer, Object> entry) {
            sleepMillis(10);
            entry.setValue(v);
            return null;
        }
    }
}
