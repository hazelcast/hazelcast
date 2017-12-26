package com.hazelcast.dataset.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.config.DataSetConfig;
import com.hazelcast.dataset.AggregationRecipe;
import com.hazelcast.dataset.EntryProcessorRecipe;
import com.hazelcast.dataset.ProjectionRecipe;
import com.hazelcast.dataset.impl.aggregation.AggregationSegmentRun;
import com.hazelcast.dataset.impl.aggregation.AggregationSegmentRunCodegen;
import com.hazelcast.dataset.impl.entryprocessor.EpSegmentRun;
import com.hazelcast.dataset.impl.entryprocessor.EpSegmentRunCodegen;
import com.hazelcast.dataset.impl.projection.ProjectionSegmentRun;
import com.hazelcast.dataset.impl.projection.ProjectionSegmentRunCodegen;
import com.hazelcast.dataset.impl.query.QuerySegmentRun;
import com.hazelcast.dataset.impl.query.QuerySegmentRunCodegen;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.function.Consumer;
import com.hazelcast.util.function.Supplier;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Partition {

    private final Compiler compiler;
    private final DataSetConfig config;
    private final SerializationService serializationService;
    private final RecordModel recordModel;
    private final long maxTenuringAgeNanos;
    private final RecordEncoder encoder;

    // the segment receiving the writes.
    private Segment edenSegment;
    // the segment first in line to be evicted
    private Segment oldestTenuredSegment;
    private Segment youngestTenuredSegment;
    private int tenuredSegmentCount;

    public Partition(DataSetConfig config, SerializationService serializationService, Compiler compiler) {
        this.config = config;
        this.compiler = compiler;
        this.maxTenuringAgeNanos = config.getTenuringAgeMillis() == Long.MAX_VALUE
                ? Long.MAX_VALUE
                : MILLISECONDS.toNanos(config.getTenuringAgeMillis());
        this.serializationService = serializationService;
        this.recordModel = new RecordModel(config.getValueClass(), config.getIndices());
        this.encoder = newEncoder();

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
            aggregators = new HashMap<String, Aggregator>();
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

    public void insert(Data keyData, Object valueData) {
        ensureEdenExists();

        edenSegment.insert(keyData, valueData);
    }

    // long ugly method
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

        System.out.println("creating new eden segment");

        Segment oldEdenSegment = edenSegment;
        edenSegment = newSegment();

        if (oldEdenSegment == null) {
            return;
        }

        // tenure the oldEdenSegment.
        if (oldestTenuredSegment == null) {
            oldestTenuredSegment = oldEdenSegment;
            youngestTenuredSegment = oldEdenSegment;
        } else {
            oldEdenSegment.previous = youngestTenuredSegment;
            youngestTenuredSegment.next = oldEdenSegment;
            youngestTenuredSegment = oldEdenSegment;
        }


        tenuredSegmentCount++;

        // get rid of the oldest tenured segment if needed.
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
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
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

        return new MemoryInfo(consumedBytes, allocatedBytes, segmentsUsed);
    }

    public void compilePredicate(String compileId, Predicate predicate) {
        SegmentRunCodegen codeGenerator = new QuerySegmentRunCodegen(
                compileId, predicate, recordModel);
        codeGenerator.generate();

        compiler.compile(codeGenerator.className(), codeGenerator.getCode());
    }

    public List query(String compileId, Map<String, Object> bindings) {
        Class<QuerySegmentRun> clazz = compiler.load("QueryScan_" + compileId);
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

    public void compileProjection(String compileId, ProjectionRecipe extraction) {
        SegmentRunCodegen codegen = new ProjectionSegmentRunCodegen(
                compileId, extraction, recordModel);
        codegen.generate();

        compiler.compile(codegen.className(), codegen.getCode());
    }

    public void projection(String compileId, Map<String, Object> bindings, Consumer consumer) {
        Class<ProjectionSegmentRun> clazz = compiler.load("ProjectionScan_" + compileId);
        ProjectionSegmentRun run = newInstance(clazz);

        run.recordDataSize = recordModel.getSize();
        run.consumer = consumer;
        run.bind(bindings);
        run.runAllFullScan(edenSegment);
        run.runAllFullScan(youngestTenuredSegment);
    }

    public void compileAggregation(String compileId, AggregationRecipe aggregationRecipe) {
        SegmentRunCodegen codegen = new AggregationSegmentRunCodegen(
                compileId, aggregationRecipe, recordModel);
        codegen.generate();

        compiler.compile(codegen.className(), codegen.getCode());
    }

    public Aggregator aggregate(String compileId, Map<String, Object> bindings) {
        Class<AggregationSegmentRun> clazz = compiler.load("Aggregation_" + compileId);
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

    public void compileEntryProcessor(String compileId, EntryProcessorRecipe recipe) {
        EpSegmentRunCodegen codegen = new EpSegmentRunCodegen(
                compileId, recipe, recordModel);
        codegen.generate();

        compiler.compile(codegen.className(), codegen.getCode());
    }

    public void entryProcessor(String compileId, Map<String, Object> bindings) {
        Class<EpSegmentRun> clazz = compiler.load("EntryProcessor_" + compileId);
        EpSegmentRun run = newInstance(clazz);

        run.recordDataSize = recordModel.getSize();
        run.bind(bindings);
        run.runAllFullScan(edenSegment);
        run.runAllFullScan(youngestTenuredSegment);
    }
}
