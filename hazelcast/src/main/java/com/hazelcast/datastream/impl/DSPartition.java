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
import com.hazelcast.datastream.EntryProcessorRecipe;
import com.hazelcast.datastream.DataStreamStats;
import com.hazelcast.datastream.ProjectionRecipe;
import com.hazelcast.datastream.impl.aggregation.AggregateFJResult;
import com.hazelcast.datastream.impl.aggregation.AggregationRegionRun;
import com.hazelcast.datastream.impl.aggregation.AggregationRegionRunCodegen;
import com.hazelcast.datastream.impl.aggregation.AggregatorRecursiveTask;
import com.hazelcast.datastream.impl.encoders.DSEncoder;
import com.hazelcast.datastream.impl.encoders.HeapDataEncoder;
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
import com.hazelcast.query.Predicate;
import com.hazelcast.util.function.Consumer;
import com.hazelcast.util.function.Supplier;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.StreamSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DSPartition {

    private final Compiler compiler;
    private final int partitionId;
    private final DataStreamConfig config;
    private final InternalSerializationService serializationService;
    private final RecordModel recordModel;
    private final long maxTenuringAgeNanos;
    private final DSEncoder encoder;
    private final ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();

    // the region receiving the writes.
    private Region eden;
    // the region first in line to be evicted
    private Region oldestTenured;
    private Region youngestTenured;
    private int tenuredRegionsCount;
    private boolean frozen;
    private final DSPartitionListeners listeners;
    private long head = 0;

    public DSPartition(DSService service,
                       int partitionId,
                       DataStreamConfig config,
                       InternalSerializationService serializationService,
                       Compiler compiler) {
        this.partitionId = partitionId;
        this.config = config;
        this.compiler = compiler;
        this.maxTenuringAgeNanos = config.getTenuringAgeMillis() == Long.MAX_VALUE
                ? Long.MAX_VALUE
                : MILLISECONDS.toNanos(config.getTenuringAgeMillis());
        this.serializationService = serializationService;
        this.recordModel = createRecordModel(config);
        this.encoder = createEncoder();
        this.listeners = service.getOrCreatePartitionListeners(config.getName(), partitionId, this);
        loadRegionFiles();
    }

    private RecordModel createRecordModel(DataStreamConfig config) {
        if(config.getValueClass()==null){
            return null;
        }
        return new RecordModel(config.getValueClass(), config.getIndices());
    }

    public RecordModel model() {
        return recordModel;
    }

    public DSEncoder encoder() {
        return encoder;
    }

    public DataStreamConfig config() {
        return config;
    }

    public long head() {
        return head;
    }

    public long tail() {
        if (eden != null) {
            return eden.tail();
        }

        if (youngestTenured != null) {
            return youngestTenured.tail();
        }

        //todo;
        return head;
    }

    private void loadRegionFiles() {
        String name = config.getName();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(config.getStorageDir().toPath(), String.format(
                "%02x%s-%08x-*.region", name.length(), name, partitionId))
        ) {
            StreamSupport.stream(paths.spliterator(), false)
                    .sorted()
                    .forEach(p -> newRegion(parseOffset(p)));
        } catch (IOException e) {
            System.out.println("WARNING: Base directory for Franz is not there: " + config.getStorageDir().getAbsolutePath());
        }
    }

    private long parseOffset(Path p) {
        String offsetTemplate = "0123456789ABCDEF";
        String fileEnding = offsetTemplate + ".region";
        String fname = p.getFileName().toString();
        String offsetStr = fname.substring(fname.length() - fileEnding.length(), offsetTemplate.length());
        return Long.parseLong(offsetStr, 16);
    }

    private DSEncoder createEncoder() {
        if(recordModel == null){
            HeapDataEncoder encoder = new HeapDataEncoder();
            encoder.serializationService = serializationService;
            return encoder;
        }

        RecordEncoderCodegen codegen = new RecordEncoderCodegen(recordModel);
        codegen.generate();
//        System.out.println(codegen.getCode());
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

    private Region newRegion(long offset) {
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

        return new Region(config.getName(), partitionId, offset, recordModel, encoder, aggregators, config);
    }

    private void ensureEdenExists() {
        boolean createEden = false;

        if (eden == null) {
            // eden doesn't exist.
            createEden = true;
        } else if (maxTenuringAgeNanos != Long.MAX_VALUE
                && System.nanoTime() - eden.firstInsertNanos() < maxTenuringAgeNanos) {
            // eden is expired
            createEden = true;
        }

        if (!createEden) {
            return;
        }

        //System.out.println("creating new eden region");

        tenureEden();
        trim();
        eden = newRegion(youngestTenured != null ? youngestTenured.tail() : head);
    }

    private void tenureEden() {
        if (eden == null) {
            return;
        }

        if (oldestTenured == null) {
            oldestTenured = eden;
            head = oldestTenured.head();
            youngestTenured = eden;
        } else {
            eden.previous = youngestTenured;
            youngestTenured.next = eden;
            youngestTenured = eden;
        }

        eden = null;
        tenuredRegionsCount++;
    }

    // get rid of the oldest tenured region if needed.
    private void trim() {
        int totalRegionCount = tenuredRegionsCount + 1;

        if (totalRegionCount > config.getRegionsPerPartition()) {
            // we need to delete the oldest region

            Region victimRegion = oldestTenured;
            victimRegion.destroy();

            head = oldestTenured.head();
            if (oldestTenured == youngestTenured) {
                oldestTenured = null;
                youngestTenured = null;
            } else {
                oldestTenured = victimRegion.next;
                oldestTenured.previous = null;
            }

            tenuredRegionsCount--;
        }
    }

    public void deleteRetiredRegions() {
        if (config.getTenuringAgeMillis() == Integer.MAX_VALUE) {
            // tenuring is disabled, so there will never be retired regions to delete
            return;
        }

        if (config.getRegionsPerPartition() == Integer.MAX_VALUE) {
            // there is no limit on the number of regions per partition, so there is nothing to delete
            return;
        }

//        Region region = oldestTenured;
//        while (region != null) {
//            Segment next = region.next;
//
//
//
//            region = next;
//            //todo: also deal with youngestTenured if last
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

        if(!eden.write(valueData)){
            if(eden.dataOffset()==0){
                throw new IllegalArgumentException("object "+valueData+" too big to be written");
            }

            tenureEden();
            ensureEdenExists();
            if(!eden.write(valueData)) {
                throw new IllegalArgumentException("object "+valueData+" too big to be written");
            }
        }

        listeners.onAppend(this);
    }

    /**
     * Returns the entry count currently stored in this partition.
     *
     * @return the count.
     */
    public long count() {
        long count = 0;

        if (eden != null) {
            count += eden.count();
        }

        Region region = youngestTenured;
        while (region != null) {
            count += region.count();
            region = region.previous;
        }

        return count;
    }

    public DataStreamStats memoryInfo() {
        long consumedBytes = 0;
        long allocatedBytes = 0;
        int regionsUsed = 0;

        if (eden != null) {
            consumedBytes += eden.consumedBytes();
            allocatedBytes += eden.allocatedBytes();
            regionsUsed++;
        }

        regionsUsed += tenuredRegionsCount;

        Region region = youngestTenured;
        while (region != null) {
            consumedBytes += region.consumedBytes();
            allocatedBytes += region.allocatedBytes();
            region = region.previous;
        }

        return new DataStreamStats(consumedBytes, allocatedBytes, regionsUsed, count());
    }

    public void prepareQuery(String preparationId, Predicate predicate) {
        if(recordModel==null){
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
            run.runAllWithIndex(eden);
            run.runAllWithIndex(oldestTenured);
        } else {
            run.runAllFullScan(eden);
            run.runAllFullScan(oldestTenured);
        }
        return run.result();
    }

    public void prepareProjection(String preparationId, ProjectionRecipe extraction) {
        if(recordModel==null){
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
        run.runAllFullScan(eden);
        run.runAllFullScan(youngestTenured);
    }

    public void prepareAggregation(String preparationId, AggregationRecipe aggregationRecipe) {
        if(recordModel==null){
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

        AggregatorRecursiveTask task = new AggregatorRecursiveTask(f, youngestTenured, () -> {
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
        edenRun.runSingleFullScan(eden);

        return new AggregateFJResult(edenRun.result(), f);
    }

    public Aggregator executeAggregationPartitionThread(String preparationId, Map<String, Object> bindings) {
        Class<AggregationRegionRun> clazz = compiler.load("AggregationRegionRun_" + preparationId);
        AggregationRegionRun run = newInstance(clazz);

        run.recordDataSize = recordModel.getSize();
        run.bind(bindings);
        run.runAllFullScan(eden);
        run.runAllFullScan(youngestTenured);
        return run.result();
    }

    public Aggregator fetchAggregate(String aggregateId) {
        Aggregator aggregator = config.getAttachedAggregators().get(aggregateId).get();
        if (eden != null) {
            aggregator.combine(eden.getAggregators().get(aggregateId));
        }

        Region region = youngestTenured;
        while (region != null) {
            aggregator.combine(region.getAggregators().get(aggregateId));
            region = region.previous;
        }
        return aggregator;
    }

    public void prepareEntryProcessor(String preparationId, EntryProcessorRecipe recipe) {
        if(recordModel==null){
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
        run.runAllFullScan(eden);
        run.runAllFullScan(youngestTenured);
    }

    public void freeze() {
        frozen = true;
        tenureEden();
        trim();
    }

    public Iterator iterator() {
        // we are not including eden for now. we rely on frozen partition
        // todo: we probably want to return youngestSegment for iteration
       // return new IteratorImpl(oldestTenured);
        throw new UnsupportedOperationException();
    }

    /**
     * Finds the region which hold data with that given offset.
     *
     * @param offset
     * @return
     */
    public Region findRegion(long offset) {
        Region current = oldestTenured;
        while (current != null) {
            // System.out.println("tenured: "+current.head()+" current.tail:"+current.tail());
            if (current.head() <= offset && current.tail() >= offset) {
                return current;
            } else {
                current = current.next;
            }
        }

        if (eden != null) {
            // System.out.println("eden: "+eden.head()+" current.tail:"+eden.tail());

            if (eden.head() <= offset && eden.tail() >= offset) {
                return eden;
            }
        }

        return null;
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
