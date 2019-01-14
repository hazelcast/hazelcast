/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.projection.Projections;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.projection.Projections.singleAttribute;
import static com.hazelcast.query.TruePredicate.truePredicate;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;

public class SourcesTest extends PipelineTestSupport {
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
    public void testConstructor() {
        assertUtilityConstructor(Sources.class);
    }

    @Test
    public void fromProcessor() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        BatchSource<Integer> source = Sources.batchFromProcessor("test",
                readMapP(srcName, truePredicate(), Entry::getValue));

        // Then
        p.drawFrom(source).drainTo(sink);
        execute();
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void map_byName() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        BatchSource<Entry<String, Integer>> source = Sources.map(srcName);

        // Then
        p.drawFrom(source).drainTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void map_byRef() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        BatchSource<Entry<String, Integer>> source = Sources.map(srcMap);

        // Then
        p.drawFrom(source).drainTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void mapWithFilterAndProjection_byName() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        BatchSource<Object> source = Sources.map(srcName, truePredicate(), singleAttribute("value"));

        // Then
        p.drawFrom(source).drainTo(sink);
        execute();
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void mapWithFilterAndProjection_byRef() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        BatchSource<Integer> source = Sources.map(srcMap, truePredicate(), Projections.singleAttribute("value"));

        // Then
        p.drawFrom(source).drainTo(sink);
        execute();
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void mapWithFilterAndProjectionFn_byName() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        BatchSource<Integer> source = Sources.map(
                srcName,
                truePredicate(),
                Entry<String, Integer>::getValue);

        // Then
        p.drawFrom(source).drainTo(sink);
        execute();
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void mapWithFilterAndProjectionFn_byRef() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        BatchSource<Integer> source = Sources.map(
                srcMap,
                truePredicate(),
                Entry::getValue);

        // Then
        p.drawFrom(source).drainTo(sink);
        execute();
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void map_withProjectionToNull_then_nullsSkipped() {
        // given
        String mapName = randomName();
        IMapJet<Integer, Entry<Integer, String>> sourceMap = jet().getMap(mapName);
        range(0, itemCount).forEach(i -> sourceMap.put(i, entry(i, i % 2 == 0 ? null : String.valueOf(i))));

        // when
        BatchSource<String> source = Sources.map(mapName, truePredicate(), singleAttribute("value"));

        // then
        p.drawFrom(source).drainTo(sink);
        jet().newJob(p);
        assertTrueEventually(() -> assertEquals(
                range(0, itemCount)
                        .filter(i -> i % 2 != 0)
                        .mapToObj(String::valueOf)
                        .sorted()
                        .collect(joining("\n")),
                jet().getHazelcastInstance().<String>getList(sinkName)
                        .stream()
                        .sorted()
                        .collect(joining("\n"))
        ));
    }

    @Test
    public void remoteMap() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        BatchSource<Entry<Object, Object>> source = Sources.remoteMap(srcName, clientConfig);

        // Then
        p.drawFrom(source).drainTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void remoteMapWithFilterAndProjection() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        BatchSource<Object> source = Sources.remoteMap(
                srcName, clientConfig, truePredicate(), singleAttribute("value"));

        // Then
        p.drawFrom(source).drainTo(sink);
        execute();
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void remoteMapWithFilterAndProjectionFn() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        BatchSource<Integer> source = Sources.remoteMap(
                srcName, clientConfig, truePredicate(), Entry<String, Integer>::getValue);

        // Then
        p.drawFrom(source).drainTo(sink);
        execute();
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void cache_byName() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcCache(input);

        // When
        BatchSource<Entry<String, Integer>> source = Sources.cache(srcName);

        // Then
        p.drawFrom(source).drainTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void remoteCache() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToCache(remoteHz.getCacheManager().getCache(srcName), input);

        // When
        BatchSource<Entry<Object, Object>> source = Sources.remoteCache(srcName, clientConfig);

        // Then
        p.drawFrom(source).drainTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void list_byName() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcList(input);

        // When
        BatchSource<Integer> source = Sources.list(srcName);

        // Then
        p.drawFrom(source).drainTo(sink);
        execute();
        assertEquals(input, sinkList);
    }

    @Test
    public void list_byRef() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcList(input);

        // When
        BatchSource<Object> source = Sources.list(srcList);

        // Then
        p.drawFrom(source).drainTo(sink);
        execute();
        assertEquals(input, sinkList);
    }

    @Test
    public void remoteList() {
        // Given
        List<Integer> input = sequence(itemCount);
        remoteHz.getList(srcName).addAll(input);

        // When
        BatchSource<Object> source = Sources.remoteList(srcName, clientConfig);

        // Then
        p.drawFrom(source).drainTo(sink);
        execute();
        assertEquals(input, sinkList);
    }

    @Test
    public void socket() throws Exception {
        // Given
        try (ServerSocket socket = new ServerSocket(8176)) {
            spawn(() -> uncheckRun(() -> {
                Socket accept1 = socket.accept();
                Socket accept2 = socket.accept();
                PrintWriter writer1 = new PrintWriter(accept1.getOutputStream());
                writer1.write("hello1 \n");
                writer1.flush();
                PrintWriter writer2 = new PrintWriter(accept2.getOutputStream());
                writer2.write("hello2 \n");
                writer2.flush();
                writer1.write("world1 \n");
                writer1.write("jet1 \n");
                writer1.flush();
                writer2.write("world2 \n");
                writer2.write("jet2 \n");
                writer2.flush();
                accept1.close();
                accept2.close();
            }));

            // When
            StreamSource<String> source = Sources.socket("localhost", 8176, UTF_8);

            // Then
            p.drawFrom(source).withoutTimestamps().drainTo(sink);
            execute();
            assertEquals(6, sinkList.size());
        }
    }

    @Test
    public void files() throws Exception {
        // Given
        File directory = createTempDirectory();
        File file1 = new File(directory, randomName());
        appendToFile(file1, "hello", "world");
        File file2 = new File(directory, randomName());
        appendToFile(file2, "hello2", "world2");

        // When
        BatchSource<String> source = Sources.files(directory.getPath());

        // Then
        p.drawFrom(source).drainTo(sink);
        execute();
        int nodeCount = jet().getCluster().getMembers().size();
        assertEquals(4 * nodeCount, sinkList.size());
    }

    @Test
    @Ignore("Changes on the file is not reflected as an event from the File System, needs more investigation")
    public void fileChanges() throws Exception {
        // Given
        File directory = createTempDirectory();
        // this is a pre-existing file, should not be picked up
        File file = new File(directory, randomName());
        appendToFile(file, "hello", "pre-existing");
        sleepAtLeastMillis(50);

        // When
        StreamSource<String> source = Sources.fileWatcher(directory.getPath());

        // Then
        p.drawFrom(source).withoutTimestamps().drainTo(sink);
        Job job = jet().newJob(p);
        // wait for the processor to initialize
        assertJobStatusEventually(job, JobStatus.RUNNING);
        // pre-existing file should not be picked up
        assertEquals(0, sinkList.size());
        appendToFile(file, "third line");
        // now, only new line should be picked up
        int nodeCount = jet().getCluster().getMembers().size();
        assertTrueEventually(() -> assertEquals(nodeCount, sinkList.size()));
    }
}
