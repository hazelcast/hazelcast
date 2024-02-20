/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dataconnection.HazelcastDataConnection;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DataHolder;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.projection.Projections;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.RecordMigrationInfo;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryViewHolder;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_CURRENT;
import static com.hazelcast.projection.Projections.singleAttribute;
import static com.hazelcast.query.impl.predicates.TruePredicate.truePredicate;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(QuickTest.class)
public class SourcesTest extends PipelineTestSupport {
    private static HazelcastInstance remoteHz;
    private static HazelcastInstance remoteHz2;
    private static ClientConfig clientConfig;

    @BeforeClass
    public static void setUp() {
        Config config = new Config();
        config.setClusterName(randomName());
        config.addCacheConfig(new CacheSimpleConfig().setName("*"));
        List<HazelcastInstance> instances = createRemoteCluster(config, 2);
        remoteHz = instances.get(0);
        remoteHz2 = instances.get(1);
        clientConfig = getClientConfigForRemoteCluster(remoteHz);
    }

    @AfterClass
    public static void afterClass() {
        HazelcastInstanceFactory.terminateAll();
        HazelcastClient.shutdownAll();
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
        p.readFrom(source).writeTo(sink);
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
        p.readFrom(source).writeTo(sink);
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
        p.readFrom(source).writeTo(sink);
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
        p.readFrom(source).writeTo(sink);
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
        p.readFrom(source).writeTo(sink);
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
        p.readFrom(source).writeTo(sink);
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
        p.readFrom(source).writeTo(sink);
        execute();
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void map_withProjectionToNull_then_nullsSkipped() {
        // given
        String mapName = randomName();
        IMap<Integer, Entry<Integer, String>> sourceMap = hz().getMap(mapName);
        range(0, itemCount).forEach(i -> sourceMap.put(i, entry(i, i % 2 == 0 ? null : String.valueOf(i))));

        // when
        BatchSource<String> source = Sources.map(mapName, truePredicate(), singleAttribute("value"));

        // then
        p.readFrom(source).writeTo(sink);
        hz().getJet().newJob(p);
        assertTrueEventually(() -> assertEquals(
                range(0, itemCount)
                        .filter(i -> i % 2 != 0)
                        .mapToObj(String::valueOf)
                        .sorted()
                        .collect(joining("\n")),
                hz().<String>getList(sinkName)
                        .stream()
                        .sorted()
                        .collect(joining("\n"))
        ));
    }

    @Test
    public void remoteMapKeys() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        BatchSource<Object> source = Sources.remoteMapKeys(srcName, clientConfig);

        // Then
        p.readFrom(source).writeTo(sink);
        execute();
        List<String> expected = input.stream()
                .map(String::valueOf)
                .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void remoteMapKeys_dataConnectionName() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        String dataConnectionName = "remoteHz";
        hz().getConfig().addDataConnectionConfig(new DataConnectionConfig(dataConnectionName)
                .setType("Hz")
                .setShared(false)
                .setProperty(HazelcastDataConnection.CLIENT_XML, ImdgUtil.asXmlString(clientConfig)));
        BatchSource<Object> source = Sources.remoteMapKeys(srcName, dataConnectionName);

        // Then
        p.readFrom(source).writeTo(sink);
        execute();
        List<String> expected = input.stream()
                .map(String::valueOf)
                .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void remoteMapKeyData() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);
        SerializationService ss = getNodeEngineImpl(remoteHz).getSerializationService();

        // When
        BatchSource<DataHolder> source = Sources.remoteMapKeyData(srcName, clientConfig);
        p.readFrom(source).writeTo(sink);
        execute();

        // Then
        List<DataHolder> keyData = input.stream().map(i -> new DataHolder(ss.toData(String.valueOf(i))))
                .collect(Collectors.toList());
        assertEquals(toBag(keyData), sinkToBag());
    }

    @Test
    public void remoteMapKeyData_dataConnectionName() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);
        SerializationService ss = getNodeEngineImpl(remoteHz).getSerializationService();

        // When
        String dataConnectionName = "remoteHz";
        hz().getConfig().addDataConnectionConfig(new DataConnectionConfig(dataConnectionName)
                .setType("Hz")
                .setShared(false)
                .setProperty(HazelcastDataConnection.CLIENT_XML, ImdgUtil.asXmlString(clientConfig)));
        BatchSource<DataHolder> source = Sources.remoteMapKeyData(srcName, dataConnectionName);
        p.readFrom(source).writeTo(sink);
        execute();

        // Then
        List<DataHolder> keyData = input.stream().map(i ->
                new DataHolder(ss.toData(String.valueOf(i)))).collect(Collectors.toList());
        assertEquals(toBag(keyData), sinkToBag());
    }

    @Test
    public void remoteMap() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getMap(srcName), input);

        // When
        BatchSource<Entry<Object, Object>> source = Sources.remoteMap(srcName, clientConfig);

        // Then
        p.readFrom(source).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void remoteReplicatedMapData() {
        remoteReplicatedMapData(() -> Sources.remoteReplicatedMapData(srcName, clientConfig));
    }

    @Test
    public void remoteReplicatedMap_customBatchSize() {
        remoteReplicatedMapData(() -> Sources.remoteReplicatedMapData(srcName, clientConfig, 100));
    }

    private void remoteReplicatedMapData(Supplier<BatchSource<Entry<DataHolder, DataHolder>>> sourceSupplier) {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getReplicatedMap(srcName), input);
        SerializationService ss = getNode(remoteHz).getSerializationService();

        // When
        BatchSource<Entry<DataHolder, DataHolder>> source = sourceSupplier.get();

        // Then
        p.readFrom(source).writeTo(sink);
        execute();
        List<Entry<DataHolder, DataHolder>> expected = input.stream()
                .map(i -> entry(new DataHolder(ss.toData(String.valueOf(i))), new DataHolder(ss.toData(i))))
                .collect(toList());
        assertThat(sinkList).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void remoteReplicatedMapData_dataConnectionName() {
        remoteReplicatedMapData_dataConnectionName((dataConnectionName)
                -> Sources.remoteReplicatedMapData(srcName, dataConnectionName));
    }

    private void remoteReplicatedMapData_dataConnectionName(Function<String, BatchSource<Entry<DataHolder, DataHolder>>>
                                                                    sourceFn) {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getReplicatedMap(srcName), input);
        SerializationService ss = getNode(remoteHz).getSerializationService();

        // When
        String dataConnectionName = randomString();
        hz().getConfig().addDataConnectionConfig(new DataConnectionConfig(dataConnectionName)
                .setType("Hz")
                .setShared(false)
                .setProperty(HazelcastDataConnection.CLIENT_XML, ImdgUtil.asXmlString(clientConfig)));
        BatchSource<Entry<DataHolder, DataHolder>> source = sourceFn.apply(dataConnectionName);

        // Then
        p.readFrom(source).writeTo(sink);
        execute();
        List<Entry<DataHolder, DataHolder>> expected = input.stream()
                .map(i -> entry(new DataHolder(ss.toData(String.valueOf(i))), new DataHolder(ss.toData(i))))
                .collect(toList());
        assertThat(sinkList).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void remoteReplicatedMap_dataConnectionName_customBatchSize() {
        remoteReplicatedMapData_dataConnectionName((dataConnectionName) -> Sources.remoteReplicatedMapData(srcName,
                dataConnectionName, 100));
    }

    @Test
    public void remoteReplicatedMap_emptySourceMap() {
        remoteReplicatedMap_emptySourceMap((dataConnectionName) -> Sources.remoteReplicatedMapData(srcName,
                dataConnectionName));
    }

    @Test
    public void remoteReplicatedMap_emptySourceMap_customBatchSize() {
        remoteReplicatedMap_emptySourceMap((dataConnectionName) -> Sources.remoteReplicatedMapData(srcName,
                dataConnectionName, 100));
    }

    private void remoteReplicatedMap_emptySourceMap(Function<String, BatchSource<Entry<DataHolder, DataHolder>>> sourceFn) {
        // Given empty map
        // When
        String dataConnectionName = randomString();
        hz().getConfig().addDataConnectionConfig(new DataConnectionConfig(dataConnectionName)
                .setType("Hz")
                .setShared(false)
                .setProperty(HazelcastDataConnection.CLIENT_XML, ImdgUtil.asXmlString(clientConfig)));
        BatchSource<Entry<DataHolder, DataHolder>> source = sourceFn.apply(dataConnectionName);

        // Then
        p.readFrom(source).writeTo(sink);
        execute();
        assertThat(sinkList).isEmpty();
    }

    @Test
    public void remoteReplicatedMap_dataConnectionMissing() {
        remoteReplicatedMap_dataConnectionMissing((dataConnectionName) -> Sources.remoteReplicatedMapData(srcName,
                dataConnectionName));
    }

    @Test
    public void remoteReplicatedMap_dataConnectionMissing_customBatchSize() {
        remoteReplicatedMap_dataConnectionMissing((dataConnectionName) -> Sources.remoteReplicatedMapData(srcName,
                dataConnectionName, 100));
    }

    private void remoteReplicatedMap_dataConnectionMissing(Function<String, BatchSource<Entry<DataHolder, DataHolder>>> sourceFn) {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getReplicatedMap(srcName), input);

        // When
        String dataConnectionName = randomString();
        BatchSource<Entry<DataHolder, DataHolder>> source = sourceFn.apply(dataConnectionName);

        // Then
        p.readFrom(source).writeTo(sink);
        assertThatThrownBy(this::execute).isInstanceOf(CompletionException.class)
                .hasStackTraceContaining("Data connection '" + dataConnectionName + "' not found");
    }

    @Test
    public void remoteReplicatedMap_dataConnectionToNonExistentCluster() {
        remoteReplicatedMap_dataConnectionToNonExistentCluster((dataConnectionName) -> Sources.remoteReplicatedMapData(srcName,
                dataConnectionName));
    }

    public void remoteReplicatedMap_dataConnectionToNonExistentCluster_customBatchSize() {
        remoteReplicatedMap_dataConnectionToNonExistentCluster((dataConnectionName) -> Sources.remoteReplicatedMapData(srcName,
                dataConnectionName, 100));
    }

    private void remoteReplicatedMap_dataConnectionToNonExistentCluster(Function<String,
            BatchSource<Entry<DataHolder, DataHolder>>> sourceFn) {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getReplicatedMap(srcName), input);

        // When
        String dataConnectionName = randomString();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("localhost:911");
        clientConfig.setClusterName("neverland");
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(3000);
        hz().getConfig().addDataConnectionConfig(new DataConnectionConfig(dataConnectionName)
                .setType("Hz")
                .setShared(false)
                .setProperty(HazelcastDataConnection.CLIENT_XML, ImdgUtil.asXmlString(clientConfig)));
        BatchSource<Entry<DataHolder, DataHolder>> source = sourceFn.apply(dataConnectionName);

        // Then
        p.readFrom(source).writeTo(sink);
        assertThatThrownBy(this::execute).isInstanceOf(CompletionException.class)
                .hasStackTraceContaining("Unable to connect to any cluster");
    }

    @Test
    public void remoteReplicatedMapEntryViews() {
        // Create map first so all record stores get created
        remoteHz.getReplicatedMap(srcName);
        // Given
        int itemCount = 100;
        int partitionId = 0;

        List<ReplicatedMapEntryViewHolder> data = null;
        for (HazelcastInstance i : new HazelcastInstance[]{remoteHz, remoteHz2}) {
            NodeEngineImpl nodeEngine = getNodeEngineImpl(i);
            if (data == null) {
                data = prepareReplicatedMapEntryViews(itemCount, partitionId,
                        nodeEngine.getPartitionService(), nodeEngine.getSerializationService());
            }
            ReplicatedMapService service = nodeEngine.getService(ReplicatedMapService.SERVICE_NAME);
            service.getReplicatedRecordStore(srcName, true, partitionId).putRecords(convertToRecordMigrationInfo(data), 0);
        }

        // When
        BatchSource<ReplicatedMapEntryViewHolder> source = Sources.remoteReplicatedMapEntryViews(srcName, clientConfig, 100);

        // Then
        p.readFrom(source).writeTo(sink);
        execute();
        assertThat(sinkList).usingElementComparator((obj1, obj2) -> {
            ReplicatedMapEntryViewHolder o1 = (ReplicatedMapEntryViewHolder) obj1;
            ReplicatedMapEntryViewHolder o2 = (ReplicatedMapEntryViewHolder) obj2;
            // hits and last access time are modified as we access the entries.
            if (equalsExceptLastAccessTimeAndHits(o1, o2)) {
                return 0;
            } else {
                return -1;
            }
        }).containsExactlyInAnyOrderElementsOf(data);
    }

    private List<ReplicatedMapEntryViewHolder> prepareReplicatedMapEntryViews(int itemCount, int partitionId,
                                                                              InternalPartitionService partitionService,
                                                                              SerializationService ss) {
        List<ReplicatedMapEntryViewHolder> list = new ArrayList<>(itemCount);
        long counter = 0;
        long currentItemCount = 0;
        while (currentItemCount < itemCount) {
            counter++;
            if (partitionService.getPartitionId(counter) != partitionId) {
                continue;
            }
            currentItemCount++;
            HeapData keyData = ss.toData(counter);
            list.add(new ReplicatedMapEntryViewHolder(keyData, keyData, RandomPicker.getInt(0, 100000),
                    RandomPicker.getInt(0, 100000), RandomPicker.getInt(0, 100000), RandomPicker.getInt(0, 100000),
                    RandomPicker.getInt(0, 100000)));
        }
        return list;
    }

    private Collection<RecordMigrationInfo> convertToRecordMigrationInfo(List<ReplicatedMapEntryViewHolder> holders) {
        Collection<RecordMigrationInfo> list = new ArrayList<>(holders.size());
        for (ReplicatedMapEntryViewHolder holder : holders) {
            RecordMigrationInfo migrationInfo = new RecordMigrationInfo(holder.getKey(), holder.getValue(),
                    holder.getTtlMillis());
            migrationInfo.setCreationTime(holder.getCreationTime());
            migrationInfo.setHits(holder.getHits());
            migrationInfo.setLastUpdateTime(holder.getLastUpdateTime());
            migrationInfo.setLastAccessTime(holder.getLastAccessTime());
            list.add(migrationInfo);
        }
        return list;
    }

    @Test
    public void remoteReplicatedMapEntryViews_dataConnectionName() {
        // Create map first so all record stores get created
        remoteHz.getReplicatedMap(srcName);
        // Given
        int itemCount = 2;
        int partitionId = 0;

        List<ReplicatedMapEntryViewHolder> data = null;
        for (HazelcastInstance i : new HazelcastInstance[]{remoteHz, remoteHz2}) {
            NodeEngineImpl nodeEngine = getNodeEngineImpl(i);
            if (data == null) {
                data = prepareReplicatedMapEntryViews(itemCount, partitionId,
                        nodeEngine.getPartitionService(), nodeEngine.getSerializationService());
            }
            ReplicatedMapService service = nodeEngine.getService(ReplicatedMapService.SERVICE_NAME);
            service.getReplicatedRecordStore(srcName, true, partitionId).putRecords(convertToRecordMigrationInfo(data), 0);
        }

        // When
        String dataConnectionName = "remoteHz";
        hz().getConfig().addDataConnectionConfig(new DataConnectionConfig(dataConnectionName)
                .setType("Hz")
                .setShared(false)
                .setProperty(HazelcastDataConnection.CLIENT_XML, ImdgUtil.asXmlString(clientConfig)));
        BatchSource<ReplicatedMapEntryViewHolder> source = Sources.remoteReplicatedMapEntryViews(
                srcName, dataConnectionName, 100);

        // Then
        p.readFrom(source).writeTo(sink);
        execute();
        assertThat(sinkList).usingElementComparator((obj1, obj2) -> {
            ReplicatedMapEntryViewHolder o1 = (ReplicatedMapEntryViewHolder) obj1;
            ReplicatedMapEntryViewHolder o2 = (ReplicatedMapEntryViewHolder) obj2;
            // hits and last access time are modified as we access the entries.
            if (o1.getKey().equals(o2.getKey()) && o1.getValue().equals(o2.getValue())
                    && o1.getCreationTime() == o2.getCreationTime()
                    && o1.getLastUpdateTime() == o2.getLastUpdateTime()
                    && o1.getTtlMillis() == o2.getTtlMillis()) {
                return 0;
            } else {
                return -1;
            }
        }).containsExactlyInAnyOrderElementsOf(data);
    }

    @Test
    public void remoteReplicatedMapEntryViews_emptySourceMap() {
        // Create map first so all record stores get created
        remoteHz.getReplicatedMap(srcName);
        // Given empty map
        // When
        String dataConnectionName = randomString();
        hz().getConfig().addDataConnectionConfig(new DataConnectionConfig(dataConnectionName)
                .setType("Hz")
                .setShared(false)
                .setProperty(HazelcastDataConnection.CLIENT_XML, ImdgUtil.asXmlString(clientConfig)));
        BatchSource<ReplicatedMapEntryViewHolder> source = Sources.remoteReplicatedMapEntryViews(
                srcName, dataConnectionName, 100);

        // Then
        p.readFrom(source).writeTo(sink);
        execute();
        assertThat(sinkList).isEmpty();
    }

    @Test
    public void remoteReplicatedMapEntryViews_dataConnectionMissing() {
        remoteReplicatedMapEntryViews_dataConnectionMissing((dataConnectionName) -> Sources.remoteReplicatedMapData(srcName,
                dataConnectionName));
    }

    private void remoteReplicatedMapEntryViews_dataConnectionMissing(Function<String, BatchSource<Entry<DataHolder, DataHolder>>>
                                                                             sourceFn) {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getReplicatedMap(srcName), input);

        // When
        String dataConnectionName = randomString();
        BatchSource<Entry<DataHolder, DataHolder>> source = sourceFn.apply(dataConnectionName);

        // Then
        p.readFrom(source).writeTo(sink);
        assertThatThrownBy(this::execute).isInstanceOf(CompletionException.class)
                .hasStackTraceContaining("Data connection '" + dataConnectionName + "' not found");
    }

    @Test
    public void remoteReplicatedMapEntryViews_dataConnectionToNonExistentCluster() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToMap(remoteHz.getReplicatedMap(srcName), input);

        // When
        String dataConnectionName = randomString();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("localhost:911");
        clientConfig.setClusterName("neverland");
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(3000);
        hz().getConfig().addDataConnectionConfig(new DataConnectionConfig(dataConnectionName)
                .setType("Hz")
                .setShared(false)
                .setProperty(HazelcastDataConnection.CLIENT_XML, ImdgUtil.asXmlString(clientConfig)));
        BatchSource<ReplicatedMapEntryViewHolder> source = Sources.remoteReplicatedMapEntryViews(srcName,
                dataConnectionName, 100);

        // Then
        p.readFrom(source).writeTo(sink);
        assertThatThrownBy(this::execute).isInstanceOf(CompletionException.class)
                .hasStackTraceContaining("Unable to connect to any cluster");
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
        p.readFrom(source).writeTo(sink);
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
        p.readFrom(source).writeTo(sink);
        execute();
        assertEquals(toBag(input), sinkToBag());
    }

    @Test
    public void remoteMapWithUnknownValueClass_whenQueryingIsNotNecessary() throws Exception {
        // Given
        URL jarResource = Thread.currentThread().getContextClassLoader()
                                .getResource("deployment/sample-pojo-1.0-car.jar");
        assertNotNull("jar not found", jarResource);
        ClassLoader cl = new URLClassLoader(new URL[]{jarResource});
        Class<?> personClz = cl.loadClass("com.sample.pojo.car.Car");
        Object person = personClz.getConstructor(String.class, String.class)
                                 .newInstance("make", "model");
        IMap<String, Object> map = remoteHz.getMap(srcName);
        // the class of the value is unknown to the remote IMDG member, it will be only known to Jet
        map.put("key", person);

        // When
        BatchSource<Entry<String, Object>> source = Sources.remoteMap(srcName, clientConfig);

        // Then
        p.readFrom(source).map(en -> en.getValue().toString()).writeTo(sink);
        JobConfig jobConfig = new JobConfig();
        jobConfig.addJar(jarResource);
        hz().getJet().newJob(p, jobConfig).join();
        List<Object> expected = singletonList(person.toString());
        List<Object> actual = new ArrayList<>(sinkList);
        assertEquals(expected, actual);
    }

    @Test
    public void cache_byName() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcCache(input);

        // When
        BatchSource<Entry<String, Integer>> source = Sources.cache(srcName);

        // Then
        p.readFrom(source).writeTo(sink);
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
        p.readFrom(source).writeTo(sink);
        execute();
        List<Entry<String, Integer>> expected = input.stream()
                                                     .map(i -> entry(String.valueOf(i), i))
                                                     .collect(toList());
        assertEquals(toBag(expected), sinkToBag());
    }

    @Test
    public void remoteCacheWithUnknownValueClass() throws Exception {
        // Given
        URL jarResource = Thread.currentThread().getContextClassLoader()
                                .getResource("deployment/sample-pojo-1.0-car.jar");
        assertNotNull("jar not found", jarResource);
        ClassLoader cl = new URLClassLoader(new URL[]{jarResource});
        Class<?> personClz = cl.loadClass("com.sample.pojo.car.Car");
        Object person = personClz.getConstructor(String.class, String.class)
                                 .newInstance("make", "model");
        ICache<String, Object> cache = remoteHz.getCacheManager().getCache(srcName);
        // the class of the value is unknown to the remote IMDG member, it will be only known to Jet
        cache.put("key", person);

        // When
        BatchSource<Entry<String, Object>> source = Sources.remoteCache(srcName, clientConfig);

        // Then
        p.readFrom(source).map(en -> en.getValue().toString()).writeTo(sink);
        JobConfig jobConfig = new JobConfig();
        jobConfig.addJar(jarResource);
        hz().getJet().newJob(p, jobConfig).join();
        List<Object> expected = singletonList(person.toString());
        List<Object> actual = new ArrayList<>(sinkList);
        assertEquals(expected, actual);
    }

    @Test
    public void list_byName() {
        // Given
        List<Integer> input = sequence(itemCount);
        addToSrcList(input);

        // When
        BatchSource<Integer> source = Sources.list(srcName);

        // Then
        p.readFrom(source).writeTo(sink);
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
        p.readFrom(source).writeTo(sink);
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
        p.readFrom(source).writeTo(sink);
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
            p.readFrom(source).withoutTimestamps().writeTo(sink);
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
        p.readFrom(source).writeTo(sink);
        execute();
        int nodeCount = hz().getCluster().getMembers().size();
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
        p.readFrom(source).withoutTimestamps().writeTo(sink);
        Job job = hz().getJet().newJob(p);
        // wait for the processor to initialize
        assertJobStatusEventually(job, JobStatus.RUNNING);
        // pre-existing file should not be picked up
        assertEquals(0, sinkList.size());
        appendToFile(file, "third line");
        // now, only new line should be picked up
        int nodeCount = hz().getCluster().getMembers().size();
        assertTrueEventually(() -> assertEquals(nodeCount, sinkList.size()));
    }

    @Test(expected = IllegalStateException.class)
    public void when_batchSourceUsedTwice_then_throwException() {
        // Given
        BatchSource<Entry<Object, Object>> source = Sources.map(srcName);
        p.readFrom(source);

        // When-Then
        p.readFrom(source);
    }

    @Test(expected = IllegalStateException.class)
    public void when_streamSourceUsedTwice_then_throwException() {
        // Given
        StreamSource<Entry<Object, Object>> source = Sources.mapJournal(srcName, START_FROM_CURRENT);
        p.readFrom(source);

        // When-Then
        p.readFrom(source);
    }

    public static boolean equalsExceptLastAccessTimeAndHits(ReplicatedMapEntryViewHolder o1, ReplicatedMapEntryViewHolder o2) {
        if (o1 == o2) {
            return true;
        }
        return o1.getCreationTime() == o2.getCreationTime() && o1.getLastUpdateTime() == o2.getLastUpdateTime()
                && o1.getTtlMillis() == o2.getTtlMillis() && Objects.equals(o1.getKey(), o2.getKey())
                && Objects.equals(o1.getValue(), o2.getValue());
    }
}
