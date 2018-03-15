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
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Job;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.impl.pipeline.AbstractStage.transformOf;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
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
    public void setName() {
        //Given
        String sinkName = randomName();
        String stageName = randomName();

        //When
        SinkStage stage = p
                .drawFrom(Sources.list(sinkName))
                .drainTo(Sinks.list(sinkName))
                .setName(stageName);

        //Then
        assertEquals(stageName, stage.name());
    }

    @Test
    public void setLocalParallelism() {
        //Given
        String sinkName = randomName();
        int localParallelism = 5;

        //When
        SinkStage stage = p
                .drawFrom(Sources.list(sinkName))
                .drainTo(Sinks.list(sinkName))
                .setLocalParallelism(localParallelism);

        //Then
        assertEquals(localParallelism, transformOf(stage).localParallelism());
    }


    @Test
    public void whenDrainToMultipleStagesToSingleSink_thenAllItemsShouldBeOnSink() {
        // Given
        String secondSourceName = randomName();
        List<Integer> input = sequence(ITEM_COUNT);
        addToSrcList(input);
        addToList(jet().getList(secondSourceName), input);

        // When
        BatchStage<Entry<Object, Object>> firstSource = p.drawFrom(Sources.list(srcName));
        BatchStage<Entry<Object, Object>> secondSource = p.drawFrom(Sources.list(secondSourceName));
        p.drainTo(Sinks.list(sinkName), firstSource, secondSource);
        execute();

        // Then
        assertEquals(ITEM_COUNT * 2, sinkList.size());
    }

    @Test
    public void cache() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcCache(input);

        // When
        p.drawFrom(Sources.cache(srcName))
         .drainTo(Sinks.cache(sinkName));
        execute();

        // Then
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
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcCache(input);

        // When
        p.drawFrom(Sources.cache(srcName))
         .drainTo(Sinks.remoteCache(sinkName, clientConfig));
        execute();

        // Then
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        ICache<String, Integer> remoteCache = remoteHz.getCacheManager().getCache(sinkName);
        assertEquals(expected.size(), remoteCache.size());
        expected.forEach(entry -> assertEquals(entry.getValue(), remoteCache.get(entry.getKey())));
    }

    @Test
    public void map() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        p.drawFrom(Sources.map(srcName))
         .drainTo(Sinks.map(sinkName));
        execute();

        // Then
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        Set<Entry<Object, Object>> actual = jet().getMap(sinkName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }


    @Test
    public void remoteMap() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        p.drawFrom(Sources.remoteMap(srcName, clientConfig))
         .drainTo(Sinks.remoteMap(sinkName, clientConfig));
        execute();

        // Then
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        Set<Entry<Object, Object>> actual = remoteHz.getMap(sinkName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithMerging() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        p.drawFrom(Sources.<String, Integer>map(srcName))
         .drainTo(Sinks.mapWithMerging(srcName,
                 Entry::getKey,
                 Entry::getValue,
                 (Integer oldValue, Integer newValue) -> oldValue + newValue));
        execute();

        // Then
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + i))
                                                     .collect(toList());
        Set<Entry<Object, Object>> actual = jet().getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithMerging_when_functionReturnsNull_then_keyIsRemoved() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        p.drawFrom(Sources.<String, Integer>map(srcName))
         .drainTo(Sinks.mapWithMerging(srcName, (Integer oldValue, Integer newValue) -> null));
        execute();

        // Then
        Set<Entry<Object, Object>> actual = jet().getMap(srcName).entrySet();
        assertEquals(0, actual.size());
    }

    @Test
    public void mapWithMerging_when_entryIsLocked_then_entryIsUpdatedRegardlessTheLock() {
        // Given
        srcMap.put("key", 1);
        srcMap.lock("key");

        // When
        p.drawFrom(Sources.<String, Integer>map(srcName))
         .drainTo(Sinks.mapWithMerging(srcName, (Integer oldValue, Integer newValue) -> oldValue + 1));
        execute();

        // Then
        assertEquals(1, srcMap.size());
        assertEquals(2, srcMap.get("key").intValue());
    }

    @Test
    public void mapWithMerging_when_sameKeyMerged_then_returnSum() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        jet().getList(srcName).addAll(input);

        // When
        p.drawFrom(Sources.<Integer>list(srcName))
         .map(e -> entry("listSum", e))
         .drainTo(Sinks.<Entry<String, Integer>, Integer>mapWithMerging(srcName,
                 (oldValue, newValue) -> oldValue + newValue));
        execute();

        // Then
        IMapJet<Object, Object> actual = jet().getMap(srcName);
        assertEquals(1, actual.size());
        assertEquals(((ITEM_COUNT - 1) * ITEM_COUNT) / 2, actual.get("listSum"));
    }


    @Test
    public void remoteMapWithMerging() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        p.drawFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig))
         .drainTo(Sinks.remoteMapWithMerging(srcName, clientConfig,
                 Entry::getKey,
                 Entry::getValue,
                 (Integer oldValue, Integer newValue) -> oldValue + newValue));
        execute();

        // Then
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + i))
                                                     .collect(toList());
        Set<Entry<Object, Object>> actual = remoteHz.getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void remoteMapWithMerging_when_functionReturnsNull_then_keyIsRemoved() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        p.drawFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig))
         .drainTo(Sinks.remoteMapWithMerging(srcName, clientConfig,
                 (Integer oldValue, Integer newValue) -> null));
        execute();

        // Then
        Set<Entry<Object, Object>> actual = remoteHz.getMap(srcName).entrySet();
        assertEquals(0, actual.size());
    }


    @Test
    public void mapWithUpdating() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        p.drawFrom(Sources.<String, Integer>map(srcName))
         .drainTo(Sinks.mapWithUpdating(srcName,
                 Entry::getKey,
                 (Integer value, Entry<String, Integer> item) -> value + 10));
        execute();

        // Then
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + 10))
                                                     .collect(toList());
        Set<Entry<Object, Object>> actual = jet().getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithUpdating_when_functionReturnsNull_then_keyIsRemoved() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        p.drawFrom(Sources.<String, Integer>map(srcName))
         .drainTo(Sinks.mapWithUpdating(srcName,
                 (Integer value, Entry<String, Integer> item) -> null));
        execute();

        // Then
        Set<Entry<Object, Object>> actual = jet().getMap(srcName).entrySet();
        assertEquals(0, actual.size());
    }

    @Test
    public void mapWithUpdating_when_itemDataSerializable_then_exceptionShouldNotThrown() {
        // Given
        IMapJet<Object, Object> sourceMap = jet().getMap(srcName);
        List<Integer> input = sequence(ITEM_COUNT);
        input.forEach(i -> sourceMap.put(String.valueOf(i), new DataSerializableObject(i)));

        // When
        p.drawFrom(Sources.<String, DataSerializableObject>map(srcName))
         .drainTo(Sinks.mapWithUpdating(srcName,
                 (DataSerializableObject value, Entry<String, DataSerializableObject> item) ->
                         new DataSerializableObject(value.value + item.getValue().value)));
        execute();

        // Then
        List<Entry<String, DataSerializableObject>> expected = input
                .stream()
                .map(i -> entry(String.valueOf(i), new DataSerializableObject(i * 2)))
                .collect(toList());
        Set<Entry<Object, Object>> actual = jet().getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void mapWithUpdating_when_entryIsLocked_then_entryIsUpdatedRegardlessTheLock() {
        // Given
        srcMap.put("key", 1);
        srcMap.lock("key");

        // When
        p.drawFrom(Sources.<String, Integer>map(srcName))
         .drainTo(Sinks.mapWithUpdating(srcName,
                 (Integer value, Entry<String, Integer> item) -> 2));
        execute();

        // Then
        assertEquals(1, srcMap.size());
        assertEquals(2, srcMap.get("key").intValue());
    }

    @Test
    public void remoteMapWithUpdating() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        p.drawFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig))
         .drainTo(Sinks.remoteMapWithUpdating(srcName, clientConfig,
                 Entry::getKey,
                 (Integer value, Entry<String, Integer> item) -> value + 10));
        execute();

        // Then
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + 10))
                                                     .collect(toList());
        Set<Entry<Object, Object>> actual = remoteHz.getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void remoteMapWithUpdating_when_functionReturnsNull_then_keyIsRemoved() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        p.drawFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig))
         .drainTo(Sinks.remoteMapWithUpdating(srcName, clientConfig,
                 (Integer value, Entry<String, Integer> item) -> null));
        execute();

        // Then
        Set<Entry<Object, Object>> actual = remoteHz.getMap(srcName).entrySet();
        assertEquals(0, actual.size());
    }

    @Test
    public void remoteMapWithUpdating_when_itemDataSerializable_then_exceptionShouldNotThrown() {
        // Given
        IMap<Object, Object> sourceMap = remoteHz.getMap(srcName);
        List<Integer> input = sequence(ITEM_COUNT);
        input.forEach(i -> sourceMap.put(String.valueOf(i), new DataSerializableObject(i)));

        // When
        p.drawFrom(Sources.<String, DataSerializableObject>remoteMap(srcName, clientConfig))
         .drainTo(Sinks.remoteMapWithUpdating(srcName, clientConfig,
                 (DataSerializableObject value, Entry<String, DataSerializableObject> item) ->
                         new DataSerializableObject(value.value + item.getValue().value)));
        execute();

        // Then
        List<Entry<String, DataSerializableObject>> expected = input
                .stream()
                .map(i -> entry(String.valueOf(i), new DataSerializableObject(i * 2)))
                .collect(toList());
        Set<Entry<Object, Object>> actual = remoteHz.getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }


    @Test
    public void mapWithEntryProcessor() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        p.drawFrom(Sources.<String, Integer>map(srcName))
         .drainTo(Sinks.mapWithEntryProcessor(srcName, Entry::getKey, entry -> new IncrementEntryProcessor<>(10)));
        execute();

        // Then
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + 10))
                                                     .collect(toList());
        Set<Entry<Object, Object>> actual = jet().getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));
    }

    @Test
    public void remoteMapWithEntryProcessor() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        p.drawFrom(Sources.<String, Integer>remoteMap(srcName, clientConfig))
         .drainTo(Sinks.remoteMapWithEntryProcessor(srcName, clientConfig, Entry::getKey,
                 entry -> new IncrementEntryProcessor<>(10)));
        execute();

        // Then
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i + 10))
                                                     .collect(toList());
        Set<Entry<Object, Object>> actual = remoteHz.getMap(srcName).entrySet();
        assertEquals(expected.size(), actual.size());
        expected.forEach(entry -> assertTrue(actual.contains(entry)));

    }

    @Test
    public void mapWithEntryProcessor_when_entryIsLocked_then_entryIsNotUpdated() {
        // Given
        srcMap.put("key", 1);
        srcMap.lock("key");

        // When
        p.drawFrom(Sources.<String, Integer>map(srcName))
         .drainTo(Sinks.mapWithEntryProcessor(srcName, Entry::getKey,
                 entry -> new IncrementEntryProcessor<>(10)));
        Job job = jet().newJob(p);

        // Then
        assertTrueEventually(() -> assertEquals(RUNNING, job.getStatus()));
        assertEquals(1, srcMap.size());
        assertEquals(1, srcMap.get("key").intValue());
        srcMap.unlock("key");
        assertTrueEventually(() -> assertEquals(11, srcMap.get("key").intValue()), 10);
        job.join();
    }

    @Test(expected = IllegalStateException.class)
    public void when_usedTwice_then_throwException() {
        BatchStage<Entry<Object, Object>> stage1 = p.drawFrom(Sources.map(srcName));
        BatchStage<Entry<Object, Object>> stage2 = p.drawFrom(Sources.map(srcName + '2'));
        Sink<Object> sink = Sinks.list(sinkName);
        stage1.drainTo(sink);
        stage2.drainTo(sink);
    }

    @Test
    public void when_readRemoteList() {
        // Given
        populateList(remoteHz.getList(srcName));

        // When
        p.drawFrom(Sources.remoteList(srcName, clientConfig))
         .drainTo(Sinks.list(sinkName));
        execute();

        // Then
        assertEquals(ITEM_COUNT, sinkList.size());
    }

    @Test
    public void when_writeRemoteList() {
        // Given
        populateList(srcList);

        // When
        p.drawFrom(Sources.list(srcName))
         .drainTo(Sinks.remoteList(sinkName, clientConfig));
        execute();

        // Then
        assertEquals(ITEM_COUNT, remoteHz.getList(sinkName).size());
    }

    private static void populateList(List<Object> list) {
        list.addAll(range(0, ITEM_COUNT).boxed().collect(toList()));
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
