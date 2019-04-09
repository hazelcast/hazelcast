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

package com.hazelcast.datastream.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataStreamConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.datastream.AggregationRecipe;
import com.hazelcast.datastream.EntryProcessorRecipe;
import com.hazelcast.datastream.MemoryInfo;
import com.hazelcast.datastream.ProjectionRecipe;
import com.hazelcast.datastream.impl.aggregation.AggregateFJResult;
import com.hazelcast.datastream.impl.aggregation.AggregationSegmentRun;
import com.hazelcast.datastream.impl.aggregation.AggregationSegmentRunCodegen;
import com.hazelcast.datastream.impl.aggregation.AggregatorRecursiveTask;
import com.hazelcast.datastream.impl.entryprocessor.EntryProcessorSegmentRun;
import com.hazelcast.datastream.impl.entryprocessor.EntryProcessorSegmentRunCodegen;
import com.hazelcast.datastream.impl.projection.ProjectionSegmentRun;
import com.hazelcast.datastream.impl.projection.ProjectionSegmentRunCodegen;
import com.hazelcast.datastream.impl.query.QuerySegmentRun;
import com.hazelcast.datastream.impl.query.QuerySegmentRunCodegen;
import com.hazelcast.internal.codeneneration.Compiler;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.function.Consumer;
import com.hazelcast.util.function.Supplier;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.StreamSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DSPartition {

    public static final File BASE_DIR = new File("franz");

    private final Compiler compiler;
    private int partitionId;
    private final DataStreamConfig config;
    private final SerializationService serializationService;
    private final RecordModel recordModel;
    private final long maxTenuringAgeNanos;
    private final RecordEncoder encoder;
    private final ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();

    // the segment receiving the writes.
    private Segment edenSegment;
    // the segment first in line to be evicted
    private Segment oldestTenuredSegment;
    private Segment youngestTenuredSegment;
    private int tenuredSegmentCount;
    private boolean frozen;
    private final DSPartitionListeners listeners;
    private long head = 0;

    public DSPartition(DSService dsService,
                       int partitionId,
                       DataStreamConfig config,
                       SerializationService serializationService,
                       Compiler compiler
    ) {
        this.partitionId = partitionId;
        this.config = config;
        this.compiler = compiler;
        this.maxTenuringAgeNanos = config.getTenuringAgeMillis() == Long.MAX_VALUE
                ? Long.MAX_VALUE
                : MILLISECONDS.toNanos(config.getTenuringAgeMillis());
        this.serializationService = serializationService;
        this.recordModel = new RecordModel(config.getValueClass(), config.getIndices());
        this.encoder = newEncoder();
        this.listeners = dsService.getOrCreateSubscription(config.getName(), partitionId, this);
        loadSegmentFiles();
        //System.out.println(config);

        //System.out.println("record payload size:" + recordModel.getPayloadSize());
    }

    private void loadSegmentFiles() {
        String name = config.getName();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(BASE_DIR.toPath(), String.format(
                "%02x%s-%08x-*.segment", name.length(), name, partitionId))
        ) {
            StreamSupport.stream(paths.spliterator(), false)
                         .sorted()
                         .forEach(p -> newSegment(parseOffset(p)));
        } catch (IOException e) {
            System.out.println("WARNING: Base directory for Franz is not there: " + BASE_DIR.getAbsolutePath());
        }
    }

    private long parseOffset(Path p) {
        String offsetTemplate = "0123456789ABCDEF";
        String fileEnding = offsetTemplate + ".segment";
        String fname = p.getFileName().toString();
        String offsetStr = fname.substring(fname.length() - fileEnding.length(), offsetTemplate.length());
        return Long.parseLong(offsetStr, 16);
    }

    private RecordEncoder newEncoder() {
        RecordEncoderCodegen codegen = new RecordEncoderCodegen(recordModel);
        codegen.generate();
//        System.out.println(codegen.getCode());
        Class<RecordEncoder> encoderClazz = compiler.compile(codegen.className(), codegen.getCode());
        try {
            Constructor<RecordEncoder> constructor = encoderClazz.getConstructor(RecordModel.class);
            return constructor.newInstance(recordModel);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Segment newSegment(long offset) {
        Map<String, Supplier<Aggregator>> attachedAggregators = config.getAttachedAggregators();

        Map<String, Aggregator> aggregators;
        if (attachedAggregators.isEmpty()) {
            aggregators = Collections.emptyMap();
        } else {
            aggregators = new HashMap<>();
            for (Map.Entry<String, Supplier<Aggregator>> entry : attachedAggregators.entrySet()) {
                aggregators.put(entry.getKey(), entry.getValue().get());
            }
        }

        return new Segment(
                config.getName(),
                partitionId,
                config.getInitialSegmentSize(),
                config.getMaxSegmentSize(),
                offset,
                serializationService,
                recordModel,
                encoder, aggregators);
    }

    public DataStreamConfig getConfig() {
        return config;
    }

    private void ensureEdenExists() {
        boolean createEden = false;

        if (edenSegment == null) {
            // eden doesn't exist.
            createEden = true;
        } else if (!edenSegment.ensureCapacity()) {
            // eden is full
            createEden = true;
        } else if (maxTenuringAgeNanos != Long.MAX_VALUE
                && System.nanoTime() - edenSegment.firstInsertNanos() < maxTenuringAgeNanos) {
            // eden is expired
            createEden = true;
        }

        if (!createEden) {
            return;
        }

        //System.out.println("creating new eden segment");

        tenureEden();
        trim();
        edenSegment = newSegment(youngestTenuredSegment != null ? youngestTenuredSegment.tail() : head);
    }

    private void tenureEden() {
        if (edenSegment == null) {
            return;
        }

        if (oldestTenuredSegment == null) {
            oldestTenuredSegment = edenSegment;
            head = oldestTenuredSegment.head();
            youngestTenuredSegment = edenSegment;
        } else {
            edenSegment.previous = youngestTenuredSegment;
            youngestTenuredSegment.next = edenSegment;
            youngestTenuredSegment = edenSegment;
        }

        edenSegment = null;
        tenuredSegmentCount++;
    }

    // get rid of the oldest tenured segment if needed.
    private void trim() {
        int totalSegmentCount = tenuredSegmentCount + 1;

        if (totalSegmentCount > config.getSegmentsPerPartition()) {
            // we need to delete the oldest segment

            Segment victimSegment = oldestTenuredSegment;
            victimSegment.destroy();

            head = oldestTenuredSegment.head();
            if (oldestTenuredSegment == youngestTenuredSegment) {
                oldestTenuredSegment = null;
                youngestTenuredSegment = null;
            } else {
                oldestTenuredSegment = victimSegment.next;
                oldestTenuredSegment.previous = null;
            }

            tenuredSegmentCount--;
        }
    }

    public static void main(String[] args) throws FileNotFoundException {
        Config config = new XmlConfigBuilder("/java/Tests/simple_map/hazelcast.xml").build();
        DataStreamConfig config1 = config.getDataStreamConfig("datastream");
        System.out.println(config1);
    }

    public void deleteRetiredSegments() {
        if (config.getTenuringAgeMillis() == Integer.MAX_VALUE) {
            // tenuring is disabled, so there will never be retired segments to delete
            return;
        }

        if (config.getSegmentsPerPartition() == Integer.MAX_VALUE) {
            // there is no limit on the number of segments per partition, so there is nothing to delete
            return;
        }

//        Segment segment = oldestTenuredSegment;
//        while (segment != null) {
//            Segment next = segment.next;
//
//
//
//            segment = next;
//            //todo: also deal with youngestTenuredSegment if last
//        }
    }

    private static <C> C newInstance(Class<C> clazz) {
        try {
            return (C) clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }


    public void append(Object valueData) {
        if (frozen) {
            throw new IllegalStateException("Can't append on a frozen datastream");
        }

        ensureEdenExists();

        edenSegment.insert(valueData);
        listeners.onAppend(this);
    }

    public long count() {
        long count = 0;

        if (edenSegment != null) {
            count += edenSegment.count();
        }

        Segment segment = youngestTenuredSegment;
        while (segment != null) {
            count += segment.count();
            segment = segment.previous;
        }

        return count;
    }

    public MemoryInfo memoryInfo() {
        long consumedBytes = 0;
        long allocatedBytes = 0;
        int segmentsUsed = 0;

        if (edenSegment != null) {
            consumedBytes += edenSegment.consumedBytes();
            allocatedBytes += edenSegment.allocatedBytes();
            segmentsUsed++;
        }

        segmentsUsed += tenuredSegmentCount;

        Segment segment = youngestTenuredSegment;
        while (segment != null) {
            consumedBytes += segment.consumedBytes();
            allocatedBytes += segment.allocatedBytes();
            segment = segment.previous;
        }

        return new MemoryInfo(consumedBytes, allocatedBytes, segmentsUsed, count());
    }

    public void prepareQuery(String preparationId, Predicate predicate) {
        SegmentRunCodegen codeGenerator = new QuerySegmentRunCodegen(
                preparationId, predicate, recordModel);
        codeGenerator.generate();

        compiler.compile(codeGenerator.className(), codeGenerator.getCode());
    }

    public List executeQuery(String preparationId, Map<String, Object> bindings) {
        Class<QuerySegmentRun> clazz = compiler.load("QuerySegmentRun_" + preparationId);
        QuerySegmentRun run = newInstance(clazz);

        run.recordDataSize = recordModel.getSize();
        run.bind(bindings);

        // very hacky; just want to trigger an index being used.
        if (run.indicesAvailable) {
            run.runAllWithIndex(edenSegment);
            run.runAllWithIndex(oldestTenuredSegment);
        } else {
            run.runAllFullScan(edenSegment);
            run.runAllFullScan(oldestTenuredSegment);
        }
        return run.result();
    }

    public void prepareProjection(String preparationId, ProjectionRecipe extraction) {
        SegmentRunCodegen codegen = new ProjectionSegmentRunCodegen(
                preparationId, extraction, recordModel);
        codegen.generate();

        compiler.compile(codegen.className(), codegen.getCode());
    }

    public void executeProjectionPartitionThread(String preparationId, Map<String, Object> bindings, Consumer consumer) {
        Class<ProjectionSegmentRun> clazz = compiler.load("ProjectionSegmentRun_" + preparationId);
        ProjectionSegmentRun run = newInstance(clazz);

        run.recordDataSize = recordModel.getSize();
        run.consumer = consumer;
        run.bind(bindings);
        run.runAllFullScan(edenSegment);
        run.runAllFullScan(youngestTenuredSegment);
    }

    public void prepareAggregation(String preparationId, AggregationRecipe aggregationRecipe) {
        SegmentRunCodegen codegen = new AggregationSegmentRunCodegen(
                preparationId, aggregationRecipe, recordModel);
        codegen.generate();

        compiler.compile(codegen.className(), codegen.getCode());
    }

    public AggregateFJResult executeAggregateFJ(String preparationId, Map<String, Object> bindings) {
        Class<AggregationSegmentRun> clazz = compiler.load("AggregationSegmentRun_" + preparationId);

        CompletableFuture<Aggregator> f = new CompletableFuture<>();

        AggregatorRecursiveTask task = new AggregatorRecursiveTask(f, youngestTenuredSegment, () -> {
            AggregationSegmentRun run = newInstance(clazz);
            run.recordDataSize = recordModel.getSize();
            run.bind(bindings);
            return run;
        });
        forkJoinPool.execute(task);

        //eden needs to be executed on partition thread.
        AggregationSegmentRun edenRun = newInstance(clazz);
        edenRun.recordDataSize = recordModel.getSize();
        edenRun.bind(bindings);
        edenRun.runSingleFullScan(edenSegment);

        return new AggregateFJResult(edenRun.result(), f);
    }

    public Aggregator executeAggregationPartitionThread(String preparationId, Map<String, Object> bindings) {
        Class<AggregationSegmentRun> clazz = compiler.load("AggregationSegmentRun_" + preparationId);
        AggregationSegmentRun run = newInstance(clazz);

        run.recordDataSize = recordModel.getSize();
        run.bind(bindings);
        run.runAllFullScan(edenSegment);
        run.runAllFullScan(youngestTenuredSegment);
        return run.result();
    }

    public Aggregator fetchAggregate(String aggregateId) {
        Aggregator aggregator = config.getAttachedAggregators().get(aggregateId).get();
        if (edenSegment != null) {
            aggregator.combine(edenSegment.getAggregators().get(aggregateId));
        }

        Segment segment = youngestTenuredSegment;
        while (segment != null) {
            aggregator.combine(segment.getAggregators().get(aggregateId));
            segment = segment.previous;
        }
        return aggregator;
    }

    public void prepareEntryProcessor(String preparationId, EntryProcessorRecipe recipe) {
        EntryProcessorSegmentRunCodegen codegen = new EntryProcessorSegmentRunCodegen(
                preparationId, recipe, recordModel);
        codegen.generate();

        compiler.compile(codegen.className(), codegen.getCode());
    }

    public void executeEntryProcessor(String preparationId, Map<String, Object> bindings) {
        Class<EntryProcessorSegmentRun> clazz = compiler.load("EntryProcessorSegmentRun_" + preparationId);
        EntryProcessorSegmentRun run = newInstance(clazz);

        run.recordDataSize = recordModel.getSize();
        run.bind(bindings);
        run.runAllFullScan(edenSegment);
        run.runAllFullScan(youngestTenuredSegment);
    }

    public void freeze() {
        frozen = true;
        tenureEden();
        trim();
    }

    public Iterator iterator() {
        // we are not including eden for now. we rely on frozen partition
        // todo: we probably want to return youngestSegment for iteration
        return new IteratorImpl(oldestTenuredSegment);
    }

    public long head() {
        return head;
    }

    public long tail() {
        if (edenSegment != null) {
            return edenSegment.tail();
        }

        if (youngestTenuredSegment != null) {
            return youngestTenuredSegment.tail();
        }

        //todo;
        return head;
    }

    public Segment findSegment(long offset) {
        Segment current = oldestTenuredSegment;
        while (current!=null){
            if (current.head() <= offset && current.tail() >= offset) {
                return current;
            } else {
                current = current.next;
            }
        }

        if(edenSegment!=null){
           if(edenSegment.head() <= offset && edenSegment.tail() >= offset){
               return edenSegment;
           }
        }

        return null;
    }

    class IteratorImpl implements Iterator {
        private Segment segment;
        private int recordIndex = -1;

        public IteratorImpl(Segment segment) {
            this.segment = segment;
        }

        @Override
        public boolean hasNext() {
            if (segment == null) {
                return false;
            }

            if (recordIndex == -1) {
                if (!segment.acquire()) {
                    segment = segment.next;
                    return hasNext();
                } else {
                    recordIndex = 0;
                }
            }

            if (recordIndex >= segment.count()) {
                segment.release();
                recordIndex = -1;
                segment = segment.next;
                return hasNext();
            }

            return true;
        }

        @Override
        public Object next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            Object o = encoder.newInstance();
            encoder.readRecord(o, segment.dataAddress(), recordIndex * recordModel.getPayloadSize());
            recordIndex++;
            return o;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
