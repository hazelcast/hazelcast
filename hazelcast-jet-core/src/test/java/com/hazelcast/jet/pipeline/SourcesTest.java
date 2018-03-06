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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.IMapJet;
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
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        p.<Integer>drawFrom(Sources.batchFromProcessor("test",
                readMapP(srcName, truePredicate(),
                        (DistributedFunction<Entry<String, Integer>, Integer>) Entry::getValue)))
                .drainTo(sink);
        execute();

        // Then
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void map() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        p.drawFrom(Sources.map(srcName))
         .drainTo(sink);
        execute();

        // Then
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void mapWithFilterAndProjection() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        p.drawFrom(Sources.map(srcName, truePredicate(), singleAttribute("value")))
         .drainTo(sink);
        execute();

        // Then
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void mapWithFilterAndProjectionFn() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcMap(input);

        // When
        p.drawFrom(Sources.map(
                srcName, truePredicate(),
                (DistributedFunction<Entry<String, Integer>, Integer>) Entry::getValue))
         .drainTo(sink);
        execute();

        // Then
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void map_withProjectionToNull_then_nullsSkipped() {
        // given
        String mapName = randomName();
        IMapJet<Integer, Entry<Integer, String>> sourceMap = jet().getMap(mapName);
        range(0, ITEM_COUNT).forEach(i -> sourceMap.put(i, entry(i, i % 2 == 0 ? null : String.valueOf(i))));
        BatchSource<String> source = Sources.map(mapName, truePredicate(), singleAttribute("value"));

        // when
        p.drawFrom(source)
         .drainTo(sink);
        jet().newJob(p);

        // then
        assertTrueEventually(() -> assertEquals(
                range(0, ITEM_COUNT)
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
        List<Integer> input = sequence(ITEM_COUNT);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        p.drawFrom(Sources.remoteMap(srcName, clientConfig))
         .drainTo(sink);
        execute();

        // Then
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void remoteMapWithFilterAndProjection() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        p.drawFrom(Sources.remoteMap(srcName, clientConfig, truePredicate(), singleAttribute("value")))
         .drainTo(sink);
        execute();

        // Then
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void remoteMapWithFilterAndProjectionFn() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        p.drawFrom(Sources.remoteMap(
                srcName, clientConfig, truePredicate(),
                (DistributedFunction<Entry<String, Integer>, Integer>) Entry::getValue))
         .drainTo(sink);
        execute();

        // Then
        assertEquals(toBag(input), sinkToBag());
    }


    @Test
    public void cache() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToSrcCache(input);

        // When
        p.drawFrom(Sources.cache(srcName))
         .drainTo(sink);
        execute();

        // Then
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }


    @Test
    public void remoteCache() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        putToCache(remoteHz.getCacheManager().getCache(srcName), input);

        // When
        p.drawFrom(Sources.remoteCache(srcName, clientConfig))
         .drainTo(sink);
        execute();

        // Then
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }


    @Test
    public void list() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        addToSrcList(input);

        // When
        p.drawFrom(Sources.list(srcName))
         .drainTo(sink);
        execute();

        // Then
        assertEquals(input, sinkList);
    }

    @Test
    public void remoteList() {
        // Given
        List<Integer> input = sequence(ITEM_COUNT);
        addToList(remoteHz.getList(srcName), input);

        // When
        p.drawFrom(Sources.remoteList(srcName, clientConfig))
         .drainTo(sink);
        execute();

        // Then
        assertEquals(input, sinkList);
    }

    @Test
    public void socket() throws Exception {
        // Given
        try (ServerSocket socket = new ServerSocket(8176)) {
            new Thread(() -> uncheckRun(() -> {
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
            })).start();

            // When
            p.drawFrom(Sources.socket("localhost", 8176, UTF_8))
             .drainTo(sink);
            execute();

            // Then
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
        p.drawFrom(Sources.files(directory.getPath()))
         .drainTo(sink);
        execute();

        // Then
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
        p.drawFrom(Sources.fileWatcher(directory.getPath()))
         .drainTo(sink);
        Job job = jet().newJob(p);

        // wait for the processor to initialize
        assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));

        // Then
        // pre-existing file should not be picked up
        assertEquals(0, sinkList.size());
        appendToFile(file, "third line");

        // now, only new line should be picked up
        int nodeCount = jet().getCluster().getMembers().size();
        assertTrueEventually(() -> assertEquals(nodeCount, sinkList.size()));
    }

}
