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
import com.hazelcast.config.DataStreamConfig;
import com.hazelcast.datastream.AggregationRecipe;
import com.hazelcast.datastream.DataStreamInfo;
import com.hazelcast.datastream.EntryProcessorRecipe;
import com.hazelcast.datastream.ProjectionRecipe;
import com.hazelcast.datastream.impl.aggregation.AggregateFJResult;
import com.hazelcast.datastream.impl.aggregation.AggregationRegionRun;
import com.hazelcast.datastream.impl.aggregation.AggregationRegionRunCodegen;
import com.hazelcast.datastream.impl.aggregation.AggregatorRecursiveTask;
import com.hazelcast.internal.commitlog.CommitLog;
import com.hazelcast.internal.commitlog.Encoder;
import com.hazelcast.internal.commitlog.UsageInfo;
import com.hazelcast.internal.commitlog.Region;
import com.hazelcast.internal.commitlog.HeapDataEncoder;
import com.hazelcast.datastream.impl.encoders.RecordEncoder;
import com.hazelcast.datastream.impl.encoders.RecordEncoderCodegen;
import com.hazelcast.datastream.impl.entryprocessor.EntryProcessorRegionRun;
import com.hazelcast.datastream.impl.entryprocessor.EntryProcessorRegionRunCodegen;
import com.hazelcast.datastream.impl.projection.ProjectionRegionRun;
import com.hazelcast.datastream.impl.projection.ProjectionRegionRunCodegen;
import com.hazelcast.datastream.impl.query.QueryRegionRun;
import com.hazelcast.datastream.impl.query.QueryRegionRunCodegen;
import com.hazelcast.internal.codeneneration.Compiler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.query.Predicate;
import com.hazelcast.util.function.Consumer;

import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DSPartition {

    private final Compiler compiler;
    private final CommitLog commitLog;
    private final int partitionId;
    private final DataStreamConfig config;
    private final InternalSerializationService serializationService;
    private final RecordModel recordModel;
    private final Encoder encoder;
    private final ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();

    // the region receiving the writes.
    // the region first in line to be evicted
    private boolean frozen;
    private final DSPartitionListeners listeners;

    public DSPartition(DSService service,
                       int partitionId,
                       DataStreamConfig config,
                       InternalSerializationService serializationService,
                       Compiler compiler) {

        long tenuringAgeNanos = config.getTenuringAgeMillis() == Long.MAX_VALUE
                ? Long.MAX_VALUE
                : MILLISECONDS.toNanos(config.getTenuringAgeMillis());


        this.partitionId = partitionId;
        this.config = config;
        this.compiler = compiler;
        this.serializationService = serializationService;
        this.recordModel = createRecordModel();
        this.encoder = createEncoder();
        this.listeners = null;//service.getOrCreatePartitionListeners(config.getName(), partitionId, this);

        CommitLog.Context context = new CommitLog.Context()
                .encoder(encoder)
                .storageDir(config.getStorageDir())
                .initialRegionSize(config.getInitialRegionSize())
                .maxRegionSize(config.getMaxRegionSize())
                .maxRegions(config.getMaxRegionsPerPartition())
                .tenuringAgeNanos(tenuringAgeNanos);

        this.commitLog = new CommitLog(context);


        // loadRegionFiles();
    }

    public int regionCount() {
        return commitLog.regionCount();
    }

    private RecordModel createRecordModel() {
        if (config.getValueClass() == null) {
            return null;
        }
        return new RecordModel(config.getValueClass(), config.getIndices());
    }

    public RecordModel model() {
        return recordModel;
    }

    public Encoder encoder() {
        return encoder;
    }

    public DataStreamConfig config() {
        return config;
    }

    /**
     * Returns the byte-offset of the head.
     *
     * @return the byte-offset of the head.
     */
    public long head() {
        return commitLog.tail();
    }

    /**
     * Returns the byte-offset of the tail.
     *
     * @return the byte-offset of the tail.
     */
    public long tail() {
        return commitLog.tail();
    }

    public void destroy(){
        commitLog.destroy();
    }

//    private void loadRegionFiles() {
//        if (!config.isStorageEnabled()) {
//            return;
//        }
//
//        String name = config.getName();
//        try (DirectoryStream<Path> paths = Files.newDirectoryStream(config.getStorageDir().toPath(), String.format(
//                "%02x%s-%08x-*.region", name.length(), name, partitionId))
//        ) {
//            StreamSupport.stream(paths.spliterator(), false)
//                    .sorted()
//                    .forEach(p -> newRegion(parseOffset(p)));
//        } catch (IOException e) {
//            System.out.println("WARNING: Base directory for Franz is not there: " + config.getStorageDir().getAbsolutePath());
//        }
//    }
//
//    private long parseOffset(Path p) {
//        String offsetTemplate = "0123456789ABCDEF";
//        String fileEnding = offsetTemplate + ".region";
//        String fname = p.getFileName().toString();
//        String offsetStr = fname.substring(fname.length() - fileEnding.length(), offsetTemplate.length());
//        return Long.parseLong(offsetStr, 16);
//    }

    private Encoder createEncoder() {
        if (recordModel == null) {
            HeapDataEncoder encoder = new HeapDataEncoder();
            encoder.serializationService = serializationService;
            return encoder;
        }

        RecordEncoderCodegen codegen = new RecordEncoderCodegen(recordModel);
        codegen.generate();
        Class<RecordEncoder> encoderClazz = compiler.compile(codegen.className(), codegen.getCode());
        try {
            Constructor<RecordEncoder> constructor = encoderClazz.getConstructor();
            RecordEncoder encoder = constructor.newInstance();
            encoder.setRecordModel(recordModel);
            encoder.serializationService = serializationService;
            return encoder;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static <C> C newInstance(Class<C> clazz) {
        try {
            return (C) clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public DataStreamInfo info() {
        UsageInfo usage = commitLog.usage();
        return new DataStreamInfo(usage.bytesConsumed(), usage.bytesAllocated(), usage.regionsInUse(), usage.count());
    }

    public void prepareQuery(String preparationId, Predicate predicate) {
        if (recordModel == null) {
            throw new IllegalStateException("Can't create prepared query for blobs");
        }

        RegionRunCodegen codeGenerator = new QueryRegionRunCodegen(
                preparationId, predicate, recordModel);
        codeGenerator.generate();

        compiler.compile(codeGenerator.className(), codeGenerator.getCode());
    }

    public List executeQuery(String preparationId, Map<String, Object> bindings) {
        Class<QueryRegionRun> clazz = compiler.load("QueryRegionRun_" + preparationId);
        QueryRegionRun run = newInstance(clazz);

        run.recordDataSize = recordModel.getSize();
        run.bind(bindings);

        // very hacky; just want to trigger an index being used.
        if (run.indicesAvailable) {
            run.runAllWithIndex(commitLog.eden());
            run.runAllWithIndex(commitLog.oldestTenured());
        } else {
            run.runAllFullScan(commitLog.eden());
            run.runAllFullScan(commitLog.oldestTenured());
        }
        return run.result();
    }

    public void prepareProjection(String preparationId, ProjectionRecipe extraction) {
        if (recordModel == null) {
            throw new IllegalStateException("Can't prepare projection for blobs");
        }
        RegionRunCodegen codegen = new ProjectionRegionRunCodegen(
                preparationId, extraction, recordModel);
        codegen.generate();

        compiler.compile(codegen.className(), codegen.getCode());
    }

    public void executeProjectionPartitionThread(String preparationId, Map<String, Object> bindings, Consumer consumer) {
        Class<ProjectionRegionRun> clazz = compiler.load("ProjectionRegionRun_" + preparationId);
        ProjectionRegionRun run = newInstance(clazz);

        run.recordDataSize = recordModel.getSize();
        run.consumer = consumer;
        run.bind(bindings);
        run.runAllFullScan(commitLog.eden());
        run.runAllFullScan(commitLog.oldestTenured());
    }

    public void prepareAggregation(String preparationId, AggregationRecipe aggregationRecipe) {
        if (recordModel == null) {
            throw new IllegalStateException("Can't create aggregation query for blobs");
        }

        RegionRunCodegen codegen = new AggregationRegionRunCodegen(
                preparationId, aggregationRecipe, recordModel);
        codegen.generate();

        compiler.compile(codegen.className(), codegen.getCode());
    }

    public AggregateFJResult executeAggregateFJ(String preparationId, Map<String, Object> bindings) {
        Class<AggregationRegionRun> clazz = compiler.load("AggregationRegionRun_" + preparationId);

        CompletableFuture<Aggregator> f = new CompletableFuture<>();

        AggregatorRecursiveTask task = new AggregatorRecursiveTask(f, commitLog.youngestTenured(), () -> {
            AggregationRegionRun run = newInstance(clazz);
            run.recordDataSize = recordModel.getSize();
            run.bind(bindings);
            return run;
        });
        forkJoinPool.execute(task);

        //eden needs to be executed on partition thread.
        AggregationRegionRun edenRun = newInstance(clazz);
        edenRun.recordDataSize = recordModel.getSize();
        edenRun.bind(bindings);
        edenRun.runSingleFullScan(commitLog.eden());

        return new AggregateFJResult(edenRun.result(), f);
    }

    public Aggregator executeAggregationPartitionThread(String preparationId, Map<String, Object> bindings) {
        Class<AggregationRegionRun> clazz = compiler.load("AggregationRegionRun_" + preparationId);
        AggregationRegionRun run = newInstance(clazz);

        run.recordDataSize = recordModel.getSize();
        run.bind(bindings);
        run.runAllFullScan(commitLog.eden());
        run.runAllFullScan(commitLog.youngestTenured());
        return run.result();
    }

    public Aggregator fetchAggregate(String aggregateId) {
//        Aggregator aggregator = config.getAttachedAggregators().get(aggregateId).get();
//        if (commitLog.eden() != null) {
//            aggregator.combine(commitLog.eden().getAggregators().get(aggregateId));
//        }
//
//        Region region = youngestTenured;
//        while (region != null) {
//            aggregator.combine(region.getAggregators().get(aggregateId));
//            region = region.previous;
//        }
//        return aggregator;
        return null;
    }

    public void prepareEntryProcessor(String preparationId, EntryProcessorRecipe recipe) {
        if (recordModel == null) {
            throw new IllegalStateException("Can't prepared entryprocessor for blobs");
        }

        EntryProcessorRegionRunCodegen codegen = new EntryProcessorRegionRunCodegen(
                preparationId, recipe, recordModel);
        codegen.generate();

        compiler.compile(codegen.className(), codegen.getCode());
    }

    public void executeEntryProcessor(String preparationId, Map<String, Object> bindings) {
        Class<EntryProcessorRegionRun> clazz = compiler.load("EntryProcessorRegionRun_" + preparationId);
        EntryProcessorRegionRun run = newInstance(clazz);

        run.recordDataSize = recordModel.getSize();
        run.bind(bindings);
        run.runAllFullScan(commitLog.eden());
        run.runAllFullScan(commitLog.youngestTenured());
    }

    public void freeze() {
        commitLog.freeze();
    }

    public Iterator iterator() {
        // we are not including eden for now. we rely on frozen partition
        // todo: we probably want to return youngestSegment for iteration
        // return new IteratorImpl(oldestTenured);
        throw new UnsupportedOperationException();
    }

    /**
     * Finds the region which hold data with that given offset.
     * <p>
     * Currently complexity is O(R) with R being the number of regions. We could
     * add a BST so we get complexity down to O(log(R)).
     * <p>
     * todo: as part of should there be an acquire? Because it could be that a
     * region is returned that just got tenured.
     * <p>
     * Also if a reference to a region is kept, a single slow consumer could keep
     * the chain alive.
     *
     * @param offset
     * @return
     */
    public Region findRegion(long offset) {
        return commitLog.findRegion(offset);
    }

    // just for testing.
    public HeapData load(long offset) {
        Region region = findRegion(offset);
        if (region == null) {
            throw new RuntimeException();
        }
        encoder.dataOffset = (int) (offset - region.head());
        encoder.dataAddress = region.dataAddress();
        return encoder.load();
    }

    public long append(Object value) {
        long offset = commitLog.append(value);
        listeners.onAppend(this);
        return offset;
    }

    public void deleteRetiredRegions() {

    }

    public long count() {
        return commitLog.count();
    }

//    class IteratorImpl implements Iterator {
//        private Segment region;
//        private int recordIndex = -1;
//
//        public IteratorImpl(Segment region) {
//            this.region = region;
//        }
//
//        @Override
//        public boolean hasNext() {
//            if (region == null) {
//                return false;
//            }
//
//            if (recordIndex == -1) {
//                if (!region.acquire()) {
//                    region = region.next;
//                    return hasNext();
//                } else {
//                    recordIndex = 0;
//                }
//            }
//
//            if (recordIndex >= region.count()) {
//                region.release();
//                recordIndex = -1;
//                region = region.next;
//                return hasNext();
//            }
//
//            return true;
//        }
//
//        @Override
//        public Object next() {
//            if (!hasNext()) {
//                throw new NoSuchElementException();
//            }
//
//            Object o = encoder.newInstance();
//            encoder.dataAddress = region.dataAddress();
//            encoder.dataOffset =
//            encoder.readRecord(o, region.dataAddress(), recordIndex * recordModel.getPayloadSize());
//            recordIndex++;
//            return o;
//        }
//
//        @Override
//        public void remove() {
//            throw new UnsupportedOperationException();
//        }
//    }
}
