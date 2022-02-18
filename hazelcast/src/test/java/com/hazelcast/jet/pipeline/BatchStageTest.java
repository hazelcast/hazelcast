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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.QuadFunction;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.Traversers.traverseItems;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.datamodel.Tuple4.tuple4;
import static com.hazelcast.jet.impl.pipeline.AbstractStage.transformOf;
import static com.hazelcast.jet.pipeline.JoinClause.joinMapEntries;
import static com.hazelcast.jet.pipeline.ServiceFactories.nonSharedService;
import static com.hazelcast.jet.pipeline.ServiceFactories.sharedService;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertAnyOrder;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertOrdered;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class BatchStageTest extends PipelineTestSupport {

    @Test(expected = IllegalArgumentException.class)
    public void when_emptyPipelineToDag_then_exceptionInIterator() {
        Pipeline.create().toDag().iterator();
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_missingSink_then_exceptionInDagIterator() {
        p.toDag().iterator();
    }

    @Test
    public void when_minimalPipeline_then_validDag() {
        batchStageFromList(emptyList()).writeTo(sink);
        assertTrue(p.toDag().iterator().hasNext());
    }

    @Test
    public void setName() {
        // Given
        String stageName = randomName();
        BatchStage<Integer> stage = batchStageFromList(emptyList());

        // When
        stage.setName(stageName);

        // Then
        assertEquals(stageName, stage.name());
    }

    @Test
    public void setLocalParallelism() {
        // Given
        int localParallelism = 10;
        BatchStage<Integer> stage = batchStageFromList(emptyList());

        // When
        stage.setLocalParallelism(localParallelism);

        // Then
        assertEquals(localParallelism, transformOf(stage).localParallelism());
    }

    @Test
    public void map() {
        // Given
        List<Integer> input = sequence(itemCount);
        FunctionEx<Integer, String> formatFn = i -> String.format("%04d-string", i);

        // When
        BatchStage<String> mapped = batchStageFromList(input).map(formatFn);

        // Then
        mapped.writeTo(sink);
        execute();
        assertEquals(streamToString(input.stream(), formatFn),
                streamToString(sinkStreamOf(String.class), identity()));
    }

    @Test
    public void apply() {
        FunctionEx<Number, String> formatFn = i -> String.format("%04d-string", i.longValue());

        // Given
        List<Integer> input = sequence(itemCount);
        FunctionEx<BatchStage<? extends Number>, BatchStage<String>> transformFn = stage ->
                stage.map(formatFn)
                        .map(String::toUpperCase);

        // When
        BatchStage<String> mapped = batchStageFromList(input)
                .apply(transformFn);

        // Then
        mapped.writeTo(sink);
        execute();
        assertEquals(streamToString(input.stream(), formatFn.andThen(String::toUpperCase)),
                streamToString(sinkStreamOf(String.class), identity()));
    }

    @Test
    public void filter() {
        // Given
        List<Integer> input = sequence(itemCount);
        PredicateEx<Integer> filterFn = i -> i % 2 == 1;

        // When
        BatchStage<Integer> filtered = batchStageFromList(input).filter(filterFn);

        // Then
        filtered.writeTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%04d-string", i);
        assertEquals(streamToString(input.stream().filter(filterFn), formatFn),
                streamToString(sinkStreamOf(Integer.class), formatFn));
    }

    @Test
    public void flatMap() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Entry<Integer, String>> flatMapped = batchStageFromList(input)
                .flatMap(i -> traverseItems(entry(i, "A"), entry(i, "B")));

        // Then
        flatMapped.writeTo(sink);
        execute();
        Function<Entry<Integer, String>, String> formatFn = e -> String.format("%04d-%s", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().flatMap(i -> Stream.of(entry(i, "A"), entry(i, "B"))), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));
    }

    @Test
    public void mapUsingService() {
        // Given
        List<Integer> input = sequence(itemCount);
        BiFunctionEx<String, Integer, String> formatFn = (s, i) -> String.format("%04d-%s", i, s);
        String suffix = "-context";

        // When
        BatchStage<String> mapped = batchStageFromList(input).mapUsingService(
                sharedService(pctx -> suffix),
                formatFn
        );

        // Then
        mapped.writeTo(sink);
        execute();
        assertEquals(
                streamToString(input.stream(), i -> formatFn.apply(suffix, i)),
                streamToString(sinkStreamOf(String.class), identity()));
    }

    @Test
    public void mapUsingServiceAsync() {
        ServiceFactory<?, ScheduledExecutorService> serviceFactory = sharedService(
                pctx -> Executors.newScheduledThreadPool(8), ExecutorService::shutdown
        );

        // Given
        List<Integer> input = sequence(itemCount);
        BiFunctionEx<String, Integer, String> formatFn = (suffix, i) -> String.format("%04d%s", i, suffix);
        String suffix = "-context";

        // When
        BatchStage<String> mapped = batchStageFromList(input).mapUsingServiceAsync(
                serviceFactory, (executor, i) -> {
                    CompletableFuture<String> f = new CompletableFuture<>();
                    executor.schedule(() -> {
                        f.complete(formatFn.apply(suffix, i));
                    }, 10, TimeUnit.MILLISECONDS);
                    return f;
                }
        );

        // Then
        mapped.writeTo(sink);
        execute();
        assertEquals(
                streamToString(input.stream().map(i -> formatFn.apply(suffix, i)), identity()),
                streamToString(sinkList.stream(), Object::toString));
    }

    @Test
    public void mapUsingServiceAsyncBatched() {
        ServiceFactory<?, ScheduledExecutorService> serviceFactory = sharedService(
                pctx -> Executors.newScheduledThreadPool(8), ExecutorService::shutdown
        );

        // Given
        List<Integer> input = sequence(itemCount);
        BiFunctionEx<String, Integer, String> formatFn = (suffix, i) -> String.format("%04d%s", i, suffix);
        String suffix = "-context";

        // When
        int batchSize = 4;
        BatchStage<String> mapped = batchStageFromList(input).mapUsingServiceAsyncBatched(serviceFactory, batchSize,
                (executor, list) -> {
                    CompletableFuture<List<String>> f = new CompletableFuture<>();
                    assertTrue("list size", list.size() <= batchSize && list.size() > 0);
                    executor.schedule(() -> {
                        List<String> result = list.stream().map(i -> formatFn.apply(suffix, i)).collect(toList());
                        f.complete(result);
                    }, 10, TimeUnit.MILLISECONDS);
                    return f;
                }
        );

        // Then
        mapped.writeTo(sink);
        execute();
        assertEquals(
                streamToString(input.stream().map(i -> formatFn.apply(suffix, i)), identity()),
                streamToString(sinkList.stream(), Object::toString));
    }

    @Test
    public void mapUsingServiceAsyncBatched_withFiltering() {
        ServiceFactory<?, ScheduledExecutorService> serviceFactory = sharedService(
                pctx -> Executors.newScheduledThreadPool(8), ExecutorService::shutdown
        );

        // Given
        List<Integer> input = sequence(itemCount);
        BiFunctionEx<String, Integer, String> formatFn = (suffix, i) -> String.format("%04d%s", i, suffix);
        String suffix = "-context";

        // When
        int batchSize = 4;
        BatchStage<String> mapped = batchStageFromList(input).mapUsingServiceAsyncBatched(serviceFactory, batchSize,
                (executor, list) -> {
                    CompletableFuture<List<String>> f = new CompletableFuture<>();
                    assertTrue("list size", list.size() <= batchSize && list.size() > 0);
                    executor.schedule(() -> {
                        List<String> result = list.stream()
                                .map(i -> i % 13 == 0 ? null : formatFn.apply(suffix, i))
                                .collect(toList());
                        f.complete(result);
                    }, 10, TimeUnit.MILLISECONDS);
                    return f;
                }
        );

        // Then
        mapped.writeTo(sink);
        execute();
        assertEquals(
                streamToString(input.stream()
                        .filter(i -> i % 13 != 0)
                        .map(i -> formatFn.apply(suffix, i)), identity()),
                streamToString(sinkList.stream(), Object::toString));
    }

    @Test
    public void mapUsingService_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);
        BiFunctionEx<Integer, String, String> formatFn = (i, s) -> String.format("%04d-%s", i, s);
        String suffix = "-keyed-context";

        // When
        BatchStage<String> mapped = batchStageFromList(input)
                .groupingKey(i -> i)
                .mapUsingService(
                        sharedService(pctx -> suffix),
                        (service, k, i) -> formatFn.apply(i, service)
                );

        // Then
        mapped.writeTo(sink);
        execute();
        assertEquals(
                streamToString(input.stream(), i -> formatFn.apply(i, suffix)),
                streamToString(sinkStreamOf(String.class), identity()));
    }

    @Test
    public void mapUsingServiceAsync_keyed() {
        ServiceFactory<?, ScheduledExecutorService> serviceFactory = sharedService(
                pctx -> Executors.newScheduledThreadPool(8), ExecutorService::shutdown
        );

        // Given
        List<Integer> input = sequence(itemCount);
        BiFunctionEx<String, Integer, String> formatFn = (suffix, i) -> String.format("%04d%s", i, suffix);
        String suffix = "-context";

        // When
        BatchStage<String> mapped = batchStageFromList(input)
                .groupingKey(i -> i)
                .mapUsingServiceAsync(
                        serviceFactory,
                        (executor, k, i) -> {
                            CompletableFuture<String> f = new CompletableFuture<>();
                            executor.schedule(() -> {
                                f.complete(formatFn.apply(suffix, i));
                            }, 10, TimeUnit.MILLISECONDS);
                            return f;
                        }
                );

        // Then
        mapped.writeTo(sink);
        execute();
        assertEquals(
                streamToString(input.stream(), i -> formatFn.apply(suffix, i)),
                streamToString(sinkStreamOf(String.class), identity()));
    }

    @Test
    public void mapUsingServiceAsyncBatched_keyed_keysExposed() {
        ServiceFactory<?, ScheduledExecutorService> serviceFactory = sharedService(
                pctx -> Executors.newScheduledThreadPool(8), ExecutorService::shutdown
        );

        // Given
        List<Integer> input = sequence(itemCount);
        BiFunctionEx<String, Integer, String> formatFn = (suffix, i) -> String.format("%04d%s", i, suffix);
        String suffix = "-context";

        // When
        int batchSize = 4;
        BatchStage<String> mapped = batchStageFromList(input)
                .groupingKey(i -> i)
                .mapUsingServiceAsyncBatched(
                        serviceFactory,
                        batchSize,
                        (executor, keys, items) -> {
                            CompletableFuture<List<String>> f = new CompletableFuture<>();
                            assertTrue("list size", items.size() <= batchSize && items.size() > 0);
                            assertEquals("lists size equality", items.size(), keys.size());
                            executor.schedule(() -> {
                                List<String> result = items.stream()
                                        .map(i -> formatFn.apply(suffix, i))
                                        .collect(toList());
                                f.complete(result);
                            }, 10, TimeUnit.MILLISECONDS);
                            return f;
                        }
                );

        // Then
        mapped.writeTo(sink);
        execute();
        assertEquals(
                streamToString(input.stream(), i -> formatFn.apply(suffix, i)),
                streamToString(sinkStreamOf(String.class), identity()));
    }

    @Test
    public void mapUsingServiceAsyncBatched_keyed_keysHidden() {
        ServiceFactory<?, ScheduledExecutorService> serviceFactory = sharedService(
                pctx -> Executors.newScheduledThreadPool(8), ExecutorService::shutdown
        );

        // Given
        List<Integer> input = sequence(itemCount);
        BiFunctionEx<String, Integer, String> formatFn = (suffix, i) -> String.format("%04d%s", i, suffix);
        String suffix = "-context";

        // When
        int batchSize = 4;
        BatchStage<String> mapped = batchStageFromList(input)
                .groupingKey(i -> i)
                .mapUsingServiceAsyncBatched(
                        serviceFactory,
                        batchSize,
                        (executor, items) -> {
                            CompletableFuture<List<String>> f = new CompletableFuture<>();
                            assertTrue("list size", items.size() <= batchSize && items.size() > 0);
                            executor.schedule(() -> {
                                List<String> result = items.stream()
                                        .map(i -> formatFn.apply(suffix, i))
                                        .collect(toList());
                                f.complete(result);
                            }, 10, TimeUnit.MILLISECONDS);
                            return f;
                        }
                );

        // Then
        mapped.writeTo(sink);
        execute();
        assertEquals(
                streamToString(input.stream(), i -> formatFn.apply(suffix, i)),
                streamToString(sinkStreamOf(String.class), identity()));
    }

    @Test
    public void mapUsingServiceAsyncBatched_keyed_withFiltering() {
        ServiceFactory<?, ScheduledExecutorService> serviceFactory = sharedService(
                pctx -> Executors.newScheduledThreadPool(8), ExecutorService::shutdown
        );

        // Given
        List<Integer> input = sequence(itemCount);
        BiFunctionEx<String, Integer, String> formatFn = (suffix, i) -> String.format("%04d%s", i, suffix);
        String suffix = "-context";

        // When
        int batchSize = 32;
        BatchStage<String> mapped = batchStageFromList(input)
                .groupingKey(i -> i % 23)
                .mapUsingServiceAsyncBatched(
                        serviceFactory,
                        batchSize,
                        (executor, keys, items) -> {
                            CompletableFuture<List<String>> f = new CompletableFuture<>();
                            assertTrue("list size", items.size() <= batchSize && items.size() > 0);
                            assertEquals("lists size equality", items.size(), keys.size());
                            executor.schedule(() -> {
                                List<String> results = items.isEmpty() ? Collections.emptyList() : new ArrayList<>();
                                for (int i = 0; i < items.size(); i++) {
                                    Integer item = items.get(i);
                                    Integer key = keys.get(i);
                                    results.add(item % 13 == 0 || key == 0 ? null : formatFn.apply(suffix, item));
                                }
                                f.complete(results);
                            }, 10, TimeUnit.MILLISECONDS);
                            return f;
                        }
                );

        // Then
        mapped.writeTo(sink);
        execute();
        assertEquals(
                streamToString(input.stream()
                        .filter(i -> i % 13 != 0 && i % 23 != 0)
                        .map(i -> formatFn.apply(suffix, i)), identity()),
                streamToString(sinkStreamOf(String.class), identity()));
    }

    @Test
    public void filterUsingService() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Integer> mapped = batchStageFromList(input).filterUsingService(
                sharedService(pctx -> 1),
                (svc, i) -> i % 2 == svc);

        // Then
        mapped.writeTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%04d-string", i);
        assertEquals(
                streamToString(input.stream().filter(i -> i % 2 == 1), formatFn),
                streamToString(sinkStreamOf(Integer.class), formatFn));
    }

    @Test
    public void filterUsingService_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Integer> mapped = batchStageFromList(input)
                .groupingKey(i -> i)
                .filterUsingService(
                        sharedService(pctx -> 1),
                        (svc, k, r) -> r % 2 == svc);

        // Then
        mapped.writeTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%04d-string", i);
        assertEquals(
                streamToString(input.stream().filter(i -> i % 2 == 1), formatFn),
                streamToString(sinkStreamOf(Integer.class), formatFn));
    }

    @Test
    public void flatMapUsingService() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Entry<Integer, String>> flatMapped = batchStageFromList(input).flatMapUsingService(
                sharedService(pctx -> asList("A", "B")),
                (ctx, i) -> traverseItems(entry(i, ctx.get(0)), entry(i, ctx.get(1))));

        // Then
        flatMapped.writeTo(sink);
        execute();
        Function<Entry<Integer, String>, String> formatFn = e -> String.format("%04d-%s", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().flatMap(i -> Stream.of(entry(i, "A"), entry(i, "B"))), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));
    }

    @Test
    public void flatMapUsingService_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Entry<Integer, String>> flatMapped = batchStageFromList(input)
                .groupingKey(i -> i)
                .flatMapUsingService(
                        sharedService(pctx -> asList("A", "B")),
                        (ctx, k, i) -> traverseItems(entry(i, ctx.get(0)), entry(i, ctx.get(1))));

        // Then
        flatMapped.writeTo(sink);
        execute();
        Function<Entry<Integer, String>, String> formatFn = e -> String.format("%04d-%s", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().flatMap(i -> Stream.of(entry(i, "A"), entry(i, "B"))), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));
    }

    @Test
    public void mapUsingReplicatedMap() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefix = "value-";
        String replicatedMapName = randomMapName();
        ReplicatedMap<Integer, String> replicatedMap = member.getReplicatedMap(replicatedMapName);
        for (int i : input) {
            replicatedMap.put(i, prefix + i);
        }
        for (HazelcastInstance hz : allHazelcastInstances()) {
            assertSizeEventually(itemCount, hz.getReplicatedMap(replicatedMapName));
        }

        // When
        BatchStage<Entry<Integer, String>> stage = batchStageFromList(input)
                .mapUsingReplicatedMap(replicatedMap, FunctionEx.identity(), Util::entry);

        // Then
        stage.writeTo(sink);
        execute();
        Function<Entry<Integer, String>, String> formatFn =
                e -> String.format("(%04d, %s)", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().map(i -> entry(i, prefix + i)), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));
    }

    @Test
    public void mapUsingIMap() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefix = "value-";
        IMap<Integer, String> map = member.getMap(randomMapName());
        for (int i : input) {
            map.put(i, prefix + i);
        }

        // When
        BatchStage<Entry<Integer, String>> stage = batchStageFromList(input)
                .mapUsingIMap(map, FunctionEx.identity(), Util::entry);

        // Then
        stage.writeTo(sink);
        execute();
        Function<Entry<Integer, String>, String> formatFn =
                e -> String.format("(%04d, %s)", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().map(i -> entry(i, prefix + i)), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));
    }

    @Test
    public void mapUsingIMap_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefix = "value-";
        IMap<Integer, String> map = member.getMap(randomMapName());
        for (int i : input) {
            map.put(i, prefix + i);
        }

        // When
        BatchStage<Entry<Integer, String>> stage = batchStageFromList(input)
                .groupingKey(i -> i)
                .mapUsingIMap(map, Util::entry);

        // Then
        stage.writeTo(sink);
        execute();
        Function<Entry<Integer, String>, String> formatFn =
                e -> String.format("(%04d, %s)", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().map(i -> entry(i, prefix + i)), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));
    }

    @Test
    public void mapUsingIMap_when_functionThrows_then_jobFails() {
        // Given
        IMap<Integer, String> map = member.getMap(randomMapName());
        map.put(1, "1");

        // When
        BatchStage<Entry<Integer, String>> stage = batchStageFromList(singletonList(1))
                .groupingKey(i -> i)
                .mapUsingIMap(map, (k, v) -> {
                    throw new RuntimeException("mock error");
                });

        // Then
        stage.writeTo(sink);
        Job job = hz().getJet().newJob(p);

        assertThatThrownBy(() -> job.join())
                .hasMessageContaining("mock error");
    }

    @Test
    public void mapStateful_global() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Long> stage = batchStageFromList(input)
                .mapStateful(LongAccumulator::new, (acc, i) -> {
                    acc.add(i);
                    return acc.get();
                });

        // Then
        stage.writeTo(sink);
        execute();
        Function<Long, String> formatFn = i -> String.format("%04d", i);
        assertEquals(
                streamToString(input.stream().map(i -> i * (i + 1L) / 2), formatFn),
                streamToString(sinkStreamOf(Long.class), formatFn)
        );
    }

    @Test
    public void mapStateful_global_returningNull() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Long> stage = batchStageFromList(input)
                .mapStateful(LongAccumulator::new, (acc, i) -> {
                    acc.add(1);
                    return (acc.get() == input.size()) ? acc.get() : null;
                });

        // Then
        stage.writeTo(assertOrdered(Collections.singletonList((long) itemCount)));
        execute();
    }

    @Test
    public void mapStateful_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Entry<Integer, Long>> stage = batchStageFromList(input)
                .groupingKey(i -> i % 2)
                .mapStateful(LongAccumulator::new, (acc, k, i) -> {
                    acc.add(i);
                    return entry(k, acc.get());
                });

        // Then
        stage.writeTo(sink);
        execute();
        Function<Entry<Integer, Long>, String> formatFn = e -> String.format("%d %04d", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().map(i -> {
                    int key = i % 2;
                    long n = i / 2 + 1;
                    return entry(key, (key + i) * n / 2);
                }), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn)
        );
    }

    @Test
    public void mapStateful_keyed_returningNull() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Entry<Integer, Long>> stage = batchStageFromList(input)
                .groupingKey(i -> i % 2)
                .mapStateful(LongAccumulator::new, (acc, k, i) -> {
                    acc.add(1);
                    if (acc.get() == input.size() / 2) {
                        return entry(k, acc.get());
                    }
                    return null;
                });

        // Then
        long expectedCount = itemCount / 2;
        stage.writeTo(assertAnyOrder(Arrays.asList(entry(0, expectedCount), entry(1, expectedCount))));
        execute();
    }

    @Test
    public void mapStateful_keyed_usedAsFilter() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Entry<Integer, Long>> stage = batchStageFromList(input)
                .groupingKey(i -> i % 2)
                .mapStateful(LongAccumulator::new, (acc, k, i) -> {
                    acc.add(1);
                    return (acc.get() == input.size() / 2) ? entry(k, acc.get()) : null;
                });

        // Then
        long expectedCount = itemCount / 2;
        stage.writeTo(assertAnyOrder(Arrays.asList(entry(0, expectedCount), entry(1, expectedCount))));
        execute();
    }

    @Test
    public void filterStateful_global() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Integer> stage = batchStageFromList(input)
                .filterStateful(LongAccumulator::new, (acc, i) -> {
                    acc.add(i);
                    return acc.get() % 2 == 0;
                });

        // Then
        stage.writeTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%04d", i);
        assertEquals(
                streamToString(
                        input.stream()
                                .filter(i -> {
                                    int sum = i * (i + 1) / 2;
                                    return sum % 2 == 0;
                                }),
                        formatFn),
                streamToString(sinkStreamOf(Integer.class), formatFn)
        );
    }

    @Test
    public void filterStateful_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Integer> stage = batchStageFromList(input)
                .groupingKey(i -> i % 2)
                .filterStateful(LongAccumulator::new, (acc, i) -> {
                    acc.add(i);
                    return acc.get() % 2 == 0;
                });

        // Then
        stage.writeTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%d %04d", i % 2, i);
        assertEquals(
                streamToString(
                        input.stream()
                                .map(i -> {
                                    // Using direct formula to sum the sequence of even/odd numbers:
                                    int first = i % 2;
                                    long count = i / 2 + 1;
                                    long sum = (first + i) * count / 2;
                                    return sum % 2 == 0 ? i : null;
                                })
                                .filter(Objects::nonNull),
                        formatFn),
                streamToString(sinkStreamOf(Integer.class), formatFn)
        );
    }

    @Test
    public void flatMapStateful_global() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Long> stage = batchStageFromList(input)
                .flatMapStateful(LongAccumulator::new, (acc, i) -> {
                    acc.add(i);
                    return traverseItems(acc.get(), acc.get());
                });

        // Then
        stage.writeTo(sink);
        execute();
        Function<Long, String> formatFn = i -> String.format("%04d", i);
        assertEquals(
                streamToString(
                        input.stream()
                                .flatMap(i -> {
                                    long sum = i * (i + 1) / 2;
                                    return Stream.of(sum, sum);
                                }),
                        formatFn),
                streamToString(sinkStreamOf(Long.class), formatFn)
        );
    }

    @Test
    public void mapStateful_global_usedAsFilter() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Long> stage = batchStageFromList(input)
                .mapStateful(LongAccumulator::new, (acc, i) -> {
                    acc.add(1);
                    return (acc.get() == input.size()) ? acc.get() : null;
                });
        // Then
        stage.writeTo(assertOrdered(Collections.singletonList((long) itemCount)));
        execute();
    }

    @Test
    public void flatMapStateful_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Entry<Integer, Long>> stage = batchStageFromList(input)
                .groupingKey(i -> i % 2)
                .flatMapStateful(LongAccumulator::new, (acc, k, i) -> {
                    acc.add(i);
                    return traverseItems(entry(k, acc.get()), entry(k, acc.get()));
                });

        // Then
        stage.writeTo(sink);
        execute();
        Function<Entry<Integer, Long>, String> formatFn = e -> String.format("%d %04d", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().flatMap(i -> {
                    int key = i % 2;
                    long n = i / 2 + 1;
                    long sum = (key + i) * n / 2;
                    return Stream.of(entry(key, sum), entry(key, sum));
                }), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn)
        );
    }

    @Test
    public void rollingAggregate_global() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Long> mapped = batchStageFromList(input).rollingAggregate(counting());

        // Then
        mapped.writeTo(sink);
        execute();
        Function<Long, String> formatFn = i -> String.format("%04d", i);
        assertEquals(
                streamToString(LongStream.range(1, itemCount + 1).boxed(), formatFn),
                streamToString(sinkStreamOf(Long.class), formatFn));
    }

    @Test
    public void rollingAggregate_keyed() {
        // Given
        itemCount = (int) roundUp(itemCount, 2);
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Entry<Integer, Long>> mapped = batchStageFromList(input)
                .groupingKey(i -> i % 2)
                .rollingAggregate(counting());

        // Then
        mapped.writeTo(sink);
        execute();
        assertEquals(0, itemCount % 2);
        Stream<Entry<Integer, Long>> expectedStream =
                LongStream.range(1, itemCount / 2 + 1)
                        .boxed()
                        .flatMap(i -> Stream.of(entry(0, i), entry(1, i)));
        Function<Entry<Integer, Long>, String> formatFn =
                e -> String.format("(%04d, %04d)", e.getKey(), e.getValue());
        assertEquals(
                streamToString(expectedStream, formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));
    }

    @Test
    public void merge() {
        // Given
        List<Integer> input = sequence(itemCount);
        BatchStage<Integer> stage0 = batchStageFromList(input);
        BatchStage<Integer> stage1 = batchStageFromList(input);

        // When
        BatchStage<Integer> merged = stage0.merge(stage1);

        // Then
        merged.writeTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%04d", i);
        assertEquals(
                streamToString(input.stream().flatMap(i -> Stream.of(i, i)), formatFn),
                streamToString(sinkStreamOf(Integer.class), formatFn));
    }

    @Test
    public void distinct() {
        // Given
        List<Integer> input = IntStream.range(0, 2 * itemCount)
                .map(i -> i % itemCount)
                .boxed().collect(toList());
        Collections.shuffle(input);

        // When
        BatchStage<Integer> distinct = batchStageFromList(input).distinct();

        // Then
        distinct.writeTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%04d", i);
        assertEquals(
                streamToString(IntStream.range(0, itemCount).boxed(), formatFn),
                streamToString(sinkStreamOf(Integer.class), formatFn));
    }

    @Test
    public void distinct_keyed() {
        // Given
        FunctionEx<Integer, Integer> keyFn = i -> i / 2;
        List<Integer> input = IntStream.range(0, 2 * itemCount).boxed().collect(toList());
        Collections.shuffle(input);

        // When
        BatchStage<Integer> distinct = batchStageFromList(input)
                .groupingKey(keyFn)
                .distinct();

        // Then
        distinct.writeTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%04d", i);
        assertEquals(
                streamToString(input.stream().map(keyFn).distinct(), formatFn),
                streamToString(sinkStreamOf(Integer.class).map(keyFn), formatFn));
    }

    @Test
    public void sort() {
        // Given
        List<Integer> input = IntStream.range(0, itemCount).boxed().collect(toList());
        List<Integer> expected = new ArrayList<>(input);
        Collections.shuffle(input);

        // When
        BatchStage<Integer> sorted = batchStageFromList(input).sort();

        // Then
        sorted.writeTo(assertOrdered(expected));
        execute();
    }

    @Test
    public void hashJoin() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefix = "value-";
        BatchStage<Entry<Integer, String>> enrichingStage =
                batchStageFromList(input).map(i -> entry(i, prefix + i));

        // When
        BatchStage<Entry<Integer, String>> joined = batchStageFromList(input).hashJoin(
                enrichingStage,
                joinMapEntries(wholeItem()),
                Util::entry);

        // Then
        joined.writeTo(sink);
        execute();
        Function<Entry<Integer, String>, String> formatFn =
                e -> String.format("(%04d, %s)", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().map(i -> tuple2(i, prefix + i)), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));
    }

    @Test
    public void hashJoin_when_outputFnReturnsNull_then_filteredOut() {
        // Given
        List<Integer> input = sequence(itemCount);
        BatchStage<Entry<Integer, String>> enrichingStage = batchStageFromList(input).map(i -> entry(i, "dud"));

        // When
        BatchStage<Entry<Integer, String>> joined = batchStageFromList(input).hashJoin(
                enrichingStage,
                joinMapEntries(wholeItem()),
                (i, enriching) -> null);

        // Then
        joined.writeTo(sink);
        execute();
        assertEquals(emptyList(), new ArrayList<>(sinkList));
    }

    @Test
    public void when_innerHashJoin_then_filterOutNulls() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefix = "value-";
        BatchStage<Entry<Integer, String>> enrichingStage = batchStageFromList(input)
                .filter(i -> i % 2 == 0)
                .map(i -> entry(i, prefix + i));

        // When
        BatchStage<Entry<Integer, String>> joined = batchStageFromList(input).innerHashJoin(
                enrichingStage,
                joinMapEntries(wholeItem()),
                Util::entry);

        // Then
        joined.writeTo(sink);
        execute();
        Function<Entry<Integer, String>, String> formatFn =
                e -> String.format("(%04d, %s)", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().filter(e -> e % 2 == 0).map(i -> tuple2(i, prefix + i)), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));
    }

    @Test
    public void when_hashJoinBuilderAddInner_then_filterOutNulls() {
        // Given
        int itemCountLocal = 16;
        List<Integer> input = sequence(itemCountLocal);
        String prefixA = "A-";
        String prefixB = "B-";
        String prefixC = "C-";
        String prefixD = "D-";
        BatchStage<Entry<Integer, String>> enrichingStage1 =
                batchStageFromList(input)
                        .filter(e -> e <= itemCountLocal / 2)
                        .flatMap(i -> traverseItems(entry(i, prefixA + i), entry(i, prefixB + i)));
        BatchStage<Entry<Integer, String>> enrichingStage2 =
                batchStageFromList(input)
                        .filter(e -> e <= itemCountLocal / 4)
                        .flatMap(i -> traverseItems(entry(i, prefixC + i)));
        BatchStage<Entry<Integer, String>> enrichingStage3 =
                batchStageFromList(input)
                        .filter(e -> e <= itemCountLocal / 8)
                        .flatMap(i -> traverseItems(entry(i, prefixD + i)));

        // When
        HashJoinBuilder<Integer> b = batchStageFromList(input).hashJoinBuilder();
        Tag<String> tagA = b.addInner(enrichingStage1, joinMapEntries(wholeItem()));
        Tag<String> tagB = b.addInner(enrichingStage2, joinMapEntries(wholeItem()));
        Tag<String> tagC = b.add(enrichingStage3, joinMapEntries(wholeItem()));
        GeneralStage<Tuple2<Integer, ItemsByTag>> joined =
                b.build(Tuple2::tuple2);

        // Then
        joined.writeTo(sink);
        execute();
        QuadFunction<Integer, String, String, String, String> formatFn =
                (i, v1, v2, v3) -> String.format("(%04d, %s, %s, %s)", i, v1, v2, v3);
        int rangeForD = itemCountLocal / 8;
        assertEquals(
                streamToString(input.stream()
                                .filter(i -> i <= itemCountLocal / 4)
                                .flatMap(i -> Stream.of(
                                        tuple4(i, prefixA, prefixC, prefixD),
                                        tuple4(i, prefixB, prefixC, prefixD)
                                )),
                        t -> formatFn.apply(t.f0(), t.f1() + t.f0(), t.f2() + t.f0(),
                                t.f0() < rangeForD ? t.f3() + t.f0() : null)),
                streamToString(sinkList.stream().map(o -> (Tuple2<Integer, ItemsByTag>) o),
                        t2 -> formatFn.apply(t2.f0(), t2.f1().get(tagA), t2.f1().get(tagB),
                                t2.f0() < rangeForD ? t2.f1().get(tagC) : "null")
                )
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void hashJoin2() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefix1 = "A-";
        String prefix2 = "B-";
        BatchStage<Entry<Integer, String>> enrichingStage1 =
                batchStageFromList(input).map(i -> entry(i, prefix1 + i));
        BatchStage<Entry<Integer, String>> enrichingStage2 =
                batchStageFromList(input).map(i -> entry(i, prefix2 + i));

        // When
        BatchStage<Tuple3<Integer, String, String>> joined = batchStageFromList(input).hashJoin2(
                enrichingStage1, joinMapEntries(wholeItem()),
                enrichingStage2, joinMapEntries(wholeItem()),
                Tuple3::tuple3
        );

        // Then
        joined.writeTo(sink);
        execute();
        Function<Tuple3<Integer, String, String>, String> formatFn =
                t3 -> String.format("(%04d, %s, %s)", t3.f0(), t3.f1(), t3.f2());
        assertEquals(
                streamToString(input.stream().map(i -> tuple3(i, prefix1 + i, prefix2 + i)), formatFn),
                streamToString(sinkList.stream().map(o -> (Tuple3<Integer, String, String>) o), formatFn)
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void hashJoinBuilder() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefixA = "A-";
        String prefixB = "B-";
        String prefixC = "C-";
        String prefixD = "D-";
        BatchStage<Entry<Integer, String>> enrichingStage1 =
                batchStageFromList(input).flatMap(i -> traverseItems(entry(i, prefixA + i), entry(i, prefixB + i)));
        BatchStage<Entry<Integer, String>> enrichingStage2 =
                batchStageFromList(input).flatMap(i -> traverseItems(entry(i, prefixC + i), entry(i, prefixD + i)));

        // When
        HashJoinBuilder<Integer> b = batchStageFromList(input).hashJoinBuilder();
        Tag<String> tagA = b.add(enrichingStage1, joinMapEntries(wholeItem()));
        Tag<String> tagB = b.add(enrichingStage2, joinMapEntries(wholeItem()));
        GeneralStage<Tuple2<Integer, ItemsByTag>> joined =
                b.build(Tuple2::tuple2);

        // Then
        joined.writeTo(sink);
        execute();
        TriFunction<Integer, String, String, String> formatFn =
                (i, v1, v2) -> String.format("(%04d, %s, %s)", i, v1, v2);
        assertEquals(
                streamToString(input.stream().flatMap(
                        i -> Stream.of(
                                tuple3(i, prefixA, prefixC),
                                tuple3(i, prefixA, prefixD),
                                tuple3(i, prefixB, prefixC),
                                tuple3(i, prefixB, prefixD))),
                        t -> formatFn.apply(t.f0(), t.f1() + t.f0(), t.f2() + t.f0())),
                streamToString(sinkList.stream().map(o -> (Tuple2<Integer, ItemsByTag>) o),
                        t2 -> formatFn.apply(t2.f0(), t2.f1().get(tagA), t2.f1().get(tagB)))
        );
    }

    @Test
    public void peekIsTransparent() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Integer> peeked = batchStageFromList(input).peek();

        // Then
        peeked.writeTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%04d", i);
        assertEquals(
                streamToString(input.stream(), formatFn),
                streamToString(sinkStreamOf(Integer.class), formatFn));
    }

    @Test
    public void peekWithToStringFunctionIsTransparent() {
        // Given
        List<Integer> input = sequence(itemCount);

        // When
        BatchStage<Integer> peeked = batchStageFromList(input).peek(Object::toString);

        // Then
        peeked.writeTo(sink);
        execute();
        Function<Integer, String> formatFn = i -> String.format("%04d", i);
        assertEquals(
                streamToString(input.stream(), formatFn),
                streamToString(sinkStreamOf(Integer.class), formatFn));
    }

    @Test
    public void customTransform() {
        // Given
        List<Integer> input = sequence(itemCount);
        FunctionEx<Integer, String> formatFn = i -> String.format("%04d", i);

        // When
        BatchStage<String> custom = batchStageFromList(input)
                .customTransform("map", Processors.mapP(formatFn));

        // Then
        custom.writeTo(sink);
        execute();
        assertEquals(
                streamToString(input.stream(), formatFn),
                streamToString(sinkStreamOf(String.class), identity()));
    }

    @Test
    public void customTransform_keyed() {
        // Given
        List<Integer> input = sequence(itemCount);
        FunctionEx<Integer, Integer> extractKeyFn = i -> i % 2;

        // When
        BatchStage<Object> custom = batchStageFromList(input)
                .groupingKey(extractKeyFn)
                .customTransform("map", Processors.mapUsingServiceP(
                        nonSharedService(pctx -> new HashSet<>()),
                        (Set<Integer> ctx, Integer item) -> {
                            Integer key = extractKeyFn.apply(item);
                            return ctx.add(key) ? key : null;
                        }));

        // Then
        custom.writeTo(sink);
        execute();
        // Each processor emitted distinct keys it observed. If groupingKey isn't correctly partitioning,
        // multiple processors will observe the same keys and the counts won't match.
        assertEquals("0\n1", streamToString(sinkStreamOf(Integer.class), Object::toString));
    }

    @Test
    public void batchAddTimestampPreserveOrderTest() {
        // Given
        List<Integer> items = IntStream.range(0, 10).boxed().collect(toList());
        BatchSource<Integer> source = TestSources.items(items);

        // When
        p.readFrom(source)
                .addTimestamps(o -> 0L, 0)
                .writeTo(assertOrdered(items));

        // Then
        execute();
    }

    @Test
    public void addTimestamps_when_upstreamHasPreferredLocalParallelism_then_lpMatchUpstream() {
        // Given
        int lp = 11;

        // When
        p.readFrom(Sources.batchFromProcessor("src",
                ProcessorMetaSupplier.of(lp, ProcessorSupplier.of(noopP()))))
                .addTimestamps(o -> 0L, 0)
                .writeTo(Sinks.noop());
        DAG dag = p.toDag();

        // Then
        Vertex tsVertex = dag.getVertex("add-timestamps");
        assertEquals(lp, tsVertex.getLocalParallelism());
    }

    @Test
    public void addTimestamps_when_upstreamHasExplicitLocalParallelism_then_lpMatchUpstream() {
        // Given
        int lp = 11;

        // When
        p.readFrom(source)
                .setLocalParallelism(lp)
                .addTimestamps(t -> System.currentTimeMillis(), 1000)
                .writeTo(Sinks.noop());
        DAG dag = p.toDag();

        // Then
        Vertex tsVertex = dag.getVertex("add-timestamps");
        assertEquals(lp, tsVertex.getLocalParallelism());
    }

    @Test
    public void addTimestamps_when_upstreamHasNoPreferredLocalParallelism_then_lpMatchUpstream() {
        // Given
        BatchSource<Object> src = Sources.batchFromProcessor("src",
                ProcessorMetaSupplier.of(noopP()));

        // When
        p.readFrom(src)
                .addTimestamps(o -> 0L, 0)
                .writeTo(Sinks.noop());
        DAG dag = p.toDag();

        // Then
        Vertex tsVertex = dag.getVertex("add-timestamps");
        Vertex srcVertex = dag.getVertex("src");
        int lp1 = srcVertex.determineLocalParallelism(-1);
        int lp2 = tsVertex.determineLocalParallelism(-1);
        assertEquals(lp1, lp2);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addTimestamps_when_HasExplicitLocalParallelism_then_throwsException() {
        // Given
        int lp = 11;

        // When
        p.readFrom(source)
                .addTimestamps(o -> 0L, 0)
                .setLocalParallelism(lp)
                .writeTo(Sinks.noop());
    }
}
