package com.hazelcast.dataset.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataSetConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.dataset.AggregationRecipe;
import com.hazelcast.dataset.EntryProcessorRecipe;
import com.hazelcast.dataset.MemoryInfo;
import com.hazelcast.dataset.ProjectionRecipe;
import com.hazelcast.dataset.impl.aggregation.AggregateFJResult;
import com.hazelcast.dataset.impl.aggregation.AggregationSegmentRun;
import com.hazelcast.dataset.impl.aggregation.AggregationSegmentRunCodegen;
import com.hazelcast.dataset.impl.entryprocessor.EntryProcessorSegmentRun;
import com.hazelcast.dataset.impl.entryprocessor.EntryProcessorSegmentRunCodegen;
import com.hazelcast.dataset.impl.projection.ProjectionSegmentRun;
import com.hazelcast.dataset.impl.projection.ProjectionSegmentRunCodegen;
import com.hazelcast.dataset.impl.query.QuerySegmentRun;
import com.hazelcast.dataset.impl.query.QuerySegmentRunCodegen;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.function.Consumer;
import com.hazelcast.util.function.Supplier;

import java.io.FileNotFoundException;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Partition {

    private final Compiler compiler;
    private final DataSetConfig config;
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

    public Partition(DataSetConfig config, SerializationService serializationService, Compiler compiler) {
        this.config = config;
        this.compiler = compiler;
        this.maxTenuringAgeNanos = config.getTenuringAgeMillis() == Long.MAX_VALUE
                ? Long.MAX_VALUE
                : MILLISECONDS.toNanos(config.getTenuringAgeMillis());
        this.serializationService = serializationService;
        this.recordModel = new RecordModel(config.getValueClass(), config.getIndices());
        this.encoder = newEncoder();

        System.out.println(config);

        System.out.println("record payload size:" + recordModel.getPayloadSize());
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

    private Segment newSegment() {
        Map<String, Supplier<Aggregator>> attachedAggregators = config.getAttachedAggregators();

        Map<String, Aggregator> aggregators;
        if (attachedAggregators.isEmpty()) {
            aggregators = Collections.EMPTY_MAP;
        } else {
            aggregators = new HashMap<>();
            for (Map.Entry<String, Supplier<Aggregator>> entry : attachedAggregators.entrySet()) {
                aggregators.put(entry.getKey(), entry.getValue().get());
            }
        }

        return new Segment(
                config.getInitialSegmentSize(),
                config.getMaxSegmentSize(),
                serializationService,
                recordModel,
                encoder, aggregators);
    }

    public DataSetConfig getConfig() {
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
        edenSegment = newSegment();
    }

    private void tenureEden() {
        if (edenSegment == null) {
            return;
        }

        if (oldestTenuredSegment == null) {
            oldestTenuredSegment = edenSegment;
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
        DataSetConfig config1 = config.getDataSetConfig("dataset");
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


    public void insert(Data keyData, Object valueData) {
        if (frozen) {
            throw new IllegalStateException("Can't insert on a frozen dataset");
        }

        ensureEdenExists();

        edenSegment.insert(keyData, valueData);
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

    public void executeProjection(String preparationId, Map<String, Object> bindings, Consumer consumer) {
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

        AggregatorRecursiveTask task = new AggregatorRecursiveTask(youngestTenuredSegment, () -> {
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

        return new AggregateFJResult(edenRun.result(), task);
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
}
