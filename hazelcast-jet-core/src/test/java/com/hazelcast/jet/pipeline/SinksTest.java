/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.Job;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.impl.pipeline.AbstractStage.transformOf;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SinksTest extends PipelineTestSupport {
    private static HazelcastInstance remoteHz;
    private static ClientConfig clientConfig;

    @BeforeClass
    public static void setUp() {
        Config config = new Config();
        config.getGroupConfig().setName(randomName());
        config.addCacheConfig(new CacheSimpleConfig().setName("*"));
        remoteHz = createRemoteCluster(config, 2).get(0);
        clientConfig = getClientConfigForRemoteCluster(remoteHz);
    }

    @AfterClass
    public static void after() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void when_setName_then_sinkHasIt() {
        //Given
        String sinkName = randomName();
        String stageName = randomName();
        SinkStage stage = p
                .drawFrom(Sources.list(sinkName))
                .drainTo(Sinks.list(sinkName));

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
                .drawFrom(Sources.list(sinkName))
                .drainTo(Sinks.list(sinkName));

        //When
        stage.setLocalParallelism(localParallelism);

        //Then
        assertEquals(localParallelism, transformOf(stage).localParallelism());
    }

    @Test
    public void whenDrainToMultipleStagesToSingleSink_thenAllItemsShouldBeOnSink() {
        // Given
        String secondSourceName = randomName();
        List<Integer> input = sequence(itemCount);
        addToSrcList(input);
        jet().getList(secondSourceName).addAll(input);
        BatchStage<Entry<Object, Object>> firstSource = p.drawFrom(Sources.list(srcName));
        BatchStage<Entry<Object, Object>> secondSource = p.drawFrom(Sources.list(secondSourceName));

        // When
        p.drainTo(Sinks.list(sinkName), firstSource, secondSource);

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
        p.drawFrom(Sources.<String, Integer>cache(srcName)).drainTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        ICache<String, Integer> cache = jet().getCacheManager().getCache(sinkName);
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
        p.drawFrom(Sources.<String, Integer>cache(srcName)).drainTo(sink);
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
        p.drawFrom(Sources.<String, Integer>map(srcName)).drainTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = jet().<String, Integer>getMap(sinkName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void map_byRef() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        IMap<String, Integer> sinkMap = jet().getMap(sinkName);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.map(sinkMap);

        // Then
        p.drawFrom(Sources.<String, Integer>map(srcName)).drainTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = sinkMap.entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void remoteMap() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.remoteMap(sinkName, clientConfig);

        // Then
        p.drawFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).drainTo(sink);
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
                (oldValue, newValue) -> oldValue + newValue);

        // Then
        p.drawFrom(Sources.<String, Integer>map(srcName)).drainTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + i))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = jet().<String, Integer>getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithMerging_byRef() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        IMap<String, Integer> srcMap = jet().getMap(srcName);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithMerging(
                srcMap,
                Entry::getKey,
                Entry::getValue,
                (oldValue, newValue) -> oldValue + newValue);

        // Then
        p.drawFrom(Sources.<String, Integer>map(srcName)).drainTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + i))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = jet().<String, Integer>getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithMerging2_byRef() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        IMap<String, Integer> srcMap = jet().getMap(srcName);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithMerging(
                srcMap, (oldValue, newValue) -> oldValue + newValue);

        // Then
        p.drawFrom(Sources.<String, Integer>map(srcName)).drainTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + i))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = jet().<String, Integer>getMap(srcName).entrySet();
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
        p.drawFrom(Sources.<String, Integer>map(srcName)).drainTo(sink);
        execute();
        Set<Entry<String, Integer>> actual = jet().<String, Integer>getMap(srcName).entrySet();
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
        p.drawFrom(Sources.<String, Integer>map(srcName)).drainTo(sink);
        execute();
        assertEquals(1, srcMap.size());
        assertEquals(2, srcMap.get("key").intValue());
    }

    @Test
    public void mapWithMerging_when_sameKey_then_valuesMerged() {
        // Given
        List<Integer> input = sequence(itemCount);
        jet().getList(srcName).addAll(input);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithMerging(srcName, (oldValue, newValue) -> oldValue + newValue);

        // Then
        p.drawFrom(Sources.<Integer>list(srcName))
         .map(e -> entry("listSum", e))
         .drainTo(sink);
        execute();
        IMap<Object, Object> actual = jet().getMap(srcName);
        assertEquals(1, actual.size());
        assertEquals(((itemCount - 1) * itemCount) / 2, actual.get("listSum"));
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
                (oldValue, newValue) -> oldValue + newValue);

        // Then
        p.drawFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).drainTo(sink);
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
        p.drawFrom(source).drainTo(sink);
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
        SinkStage sinkStage = p.drawFrom(Sources.<String, Integer>map(srcName)).drainTo(sink);
        sinkStage.setLocalParallelism(2);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + 10))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = jet().<String, Integer>getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithUpdating_byRef() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        IMap<String, Integer> srcMap = jet().getMap(srcName);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithUpdating(
                srcMap,
                Entry::getKey,
                (Integer value, Entry<String, Integer> item) -> value + 10);

        // Then
        SinkStage sinkStage = p.drawFrom(Sources.<String, Integer>map(srcName)).drainTo(sink);
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
    public void mapWithUpdating2_byRef() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        IMap<String, Integer> srcMap = jet().getMap(srcName);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithUpdating(
                srcMap,
                (Integer value, Entry<String, Integer> item) -> value + 10);

        // Then
        SinkStage sinkStage = p.drawFrom(Sources.<String, Integer>map(srcName)).drainTo(sink);
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
        p.drawFrom(Sources.<String, Integer>map(srcName)).drainTo(sink);
        execute();
        Set<Entry<String, Integer>> actual = jet().<String, Integer>getMap(srcName).entrySet();
        assertEquals(0, actual.size());
    }

    @Test
    public void mapWithUpdating_when_itemDataSerializable_then_exceptionShouldNotThrown() {
        // Given
        IMap<String, DataSerializableObject> sourceMap = jet().getMap(srcName);
        List<Integer> input = sequence(itemCount);
        input.forEach(i -> sourceMap.put(String.valueOf(i), new DataSerializableObject(i)));

        // When
        Sink<Entry<String, DataSerializableObject>> sink = Sinks.mapWithUpdating(srcName,
                (value, item) -> new DataSerializableObject(value.value + item.getValue().value));

        // Then
        p.drawFrom(Sources.<String, DataSerializableObject>map(srcName)).drainTo(sink);
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
        p.drawFrom(Sources.<String, Integer>map(srcName)).drainTo(sink);
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
        p.drawFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).drainTo(sink);
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
        p.drawFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).drainTo(sink);
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
        p.drawFrom(Sources.<String, DataSerializableObject>remoteMap(srcName, clientConfig)).drainTo(sink);
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
        p.drawFrom(Sources.<String, Integer>map(srcName)).drainTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + 10))
                                                     .collect(toList());
        Set<Entry<String, Integer>> actual = jet().<String, Integer>getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithEntryProcessor_byRef() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        IMap<String, Integer> map = jet().getMap(srcName);

        // When
        Sink<Entry<String, Integer>> sink = Sinks.mapWithEntryProcessor(
                map, Entry::getKey, entry -> new IncrementEntryProcessor<>(10));

        // Then
        p.drawFrom(Sources.<String, Integer>map(srcName)).drainTo(sink);
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
        p.drawFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig)).drainTo(sink);
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
        p.drawFrom(Sources.<String, Integer>map(srcName)).drainTo(sink);
        Job job = jet().newJob(p);
        assertTrueEventually(() -> assertEquals(RUNNING, job.getStatus()));
        assertEquals(1, srcMap.size());
        assertEquals(1, srcMap.get("key").intValue());
        srcMap.unlock("key");
        assertTrueEventually(() -> assertEquals(11, srcMap.get("key").intValue()), 10);
        job.join();
    }

    @Test(expected = IllegalStateException.class)
    public void when_usedTwice_then_throwException() {
        // Given
        BatchStage<Entry<Object, Object>> stage1 = p.drawFrom(Sources.map(srcName));
        BatchStage<Entry<Object, Object>> stage2 = p.drawFrom(Sources.map(srcName + '2'));
        Sink<Object> sink = Sinks.list(sinkName);
        stage1.drainTo(sink);

        // When
        stage2.drainTo(sink);

        // Then IllegalStateException thrown
    }

    @Test
    public void list_byName() {
        // Given
        populateList(srcList);

        // When
        Sink<Object> sink = Sinks.list(sinkName);

        // Then
        p.drawFrom(Sources.list(srcList)).drainTo(sink);
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
        p.drawFrom(Sources.list(srcList)).drainTo(sink);
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
        p.drawFrom(Sources.list(srcName)).drainTo(sink);
        execute();
        assertEquals(itemCount, remoteHz.getList(sinkName).size());
    }

    @Test
    public void noop() {
        // Given
        populateList(srcList);

        // When
        Sink<Object> sink = Sinks.noop();

        // Then
        p.drawFrom(Sources.list(srcName)).drainTo(sink);
        execute();
        // works without error
    }

    private void populateList(List<Object> list) {
        list.addAll(sequence(itemCount));
    }

    private static class IncrementEntryProcessor<K> extends AbstractEntryProcessor<K, Integer> {

        private Integer value;

        IncrementEntryProcessor(Integer value) {
            this.value = value;
        }

        @Override
        public Object process(Entry<K, Integer> entry) {
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
}
