/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.iteration.IndexIterationPointer;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.connector.AbstractIndexReader;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation.MapFetchIndexOperationResult;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation.MissingPartitionException;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.sql.impl.exec.scan.MapScanRow;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.predicate.TernaryLogic;
import com.hazelcast.sql.impl.plan.node.MapIndexScanMetadata;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.hazelcast.instance.impl.HazelcastInstanceFactory.getHazelcastInstance;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.Util.distributeObjects;
import static com.hazelcast.jet.sql.impl.ExpressionUtil.evaluate;
import static com.hazelcast.jet.sql.impl.JetSqlSerializerHook.F_ID;
import static com.hazelcast.jet.sql.impl.JetSqlSerializerHook.IMAP_INDEX_SCAN_PROCESSOR_META_SUPPLIER;
import static com.hazelcast.jet.sql.impl.JetSqlSerializerHook.IMAP_INDEX_SCAN_PROCESSOR_SUPPLIER;
import static java.util.stream.Collectors.toList;

/*
Description of MapIndexFetchOp:

Behavior of the processor
- Converts `IndexFilter` to `IndexIterationPointer[]`. Replaces `ParameterExpression` with `ConstantExpression`
- initially it assumes that all assigned partitions are local. Sends IndexFetchOp with all local partitions
- When response is received, saves the new `pointers` to `lastPointers` and
    sends another one immediately with the new pointers and then emits to processor's outbox

- behavior when MissingPartitionException is received:
  - get current partition table
  - split the partitions assigned to the processor according to the new partition table into disjoint sets,
        one for each member having some partition assigned to this processor
  - sends one IndexFetchOp to each target in parallel. The starting position will be `lastPointers`.
        From now on we have to track `lastPointers` separately for each target.
  - merge-sort the results from multiple IndexFetchOp into a single sor
        Note that the initial partitionIdSet is only split, it's never joined again.
            For example if some partition migrates away and then back, we'll still continue with two operations.
            We can split any number of times, after each `MissingPartitionException`.
*/

/**
 * // TODO: [sasha] detailed description
 */
public final class MapIndexScanP<F extends CompletableFuture<MapFetchIndexOperationResult>> extends AbstractProcessor {
    // TODO: [sasha] discuss this number.
    private static final int MAX_FETCH_SIZE = 64;

    private final AbstractIndexReader<F, MapFetchIndexOperationResult, QueryableEntry<?, ?>> reader;
    private final ExpressionEvalContext evalContext;
    private final List<Expression<?>> projections;
    private final Expression<Boolean> filter;
    private transient Context context;

    private final ArrayList<Split<F>> splits;
    private final MapScanRow row;
    private final Queue<Object[]> rowsQueue;
    private final Queue<Integer> splitsToRemove = new ArrayDeque<>();
    private final Queue<Split<F>> splitsToAdd = new ArrayDeque<>();
    private Object[] pendingItem;

    public MapIndexScanP(
            @Nonnull AbstractIndexReader<F, MapFetchIndexOperationResult, QueryableEntry<?, ?>> reader,
            @Nonnull HazelcastInstance hazelcastInstance,
            @Nonnull ExpressionEvalContext evalContext,
            @Nonnull int[] partitions,
            @Nonnull MapIndexScanMetadata indexScanMetadata
    ) {
        this.reader = reader;
        this.evalContext = evalContext;
        this.projections = indexScanMetadata.getProjections();
        this.filter = indexScanMetadata.getRemainingFilter();
        this.splits = new ArrayList<>();
        this.splits.add(
                new Split<>(
                        new PartitionIdSet(hazelcastInstance.getPartitionService().getPartitions().size(), partitions),
                        hazelcastInstance.getCluster().getLocalMember().getAddress(),
                        filtersToPointers(indexScanMetadata.getFilter())
                )
        );

        this.row = MapScanRow.create(
                indexScanMetadata.getKeyDescriptor(),
                indexScanMetadata.getValueDescriptor(),
                indexScanMetadata.getFieldPaths(),
                indexScanMetadata.getFieldTypes(),
                Extractors.newBuilder(evalContext.getSerializationService()).build(),
                evalContext.getSerializationService()
        );

        if (indexScanMetadata.getComparator() != null) {
            this.rowsQueue = new PriorityQueue<>(indexScanMetadata.getComparator());
        } else {
            this.rowsQueue = new ArrayDeque<>();
        }
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        this.context = context;
        super.init(context);
    }

    @SuppressWarnings({"rawtypes", "SingleStatementInBlock", "unchecked"})
    @Override
    public boolean complete() {
        if (pendingItem != null && !tryEmit(pendingItem)) {
            return false;
        }

        // Remove all splits which ends their job.
        while (!splitsToRemove.isEmpty()) {
            int split = splitsToRemove.poll();
            splits.remove(split);
        }

        // Add all splits which should start their execution.
        while (!splitsToAdd.isEmpty()) {
            Split split = splitsToAdd.poll();
            splits.add(split);
        }

        if (splits.isEmpty()) {
            return projectEntryAndEmit();
        }

        for (int i = 0; i < splits.size(); ++i) {
            Split s = splits.get(i);
            if (s.start(reader)) {
                continue;
            }
            if (s.batchIsDrained()) {
                if (!s.isDone()) {
                    continue;
                } else {
                    try {
                        boolean readFlag = s.readBatch(reader);
                        if (readFlag) {
                            // No more items to read, remove waisted split.
                            splitsToRemove.offer(i);
                            continue;
                        }
                    } catch (MissingPartitionException e) {
                        splitOnMigration(i);
                        continue;
                    }
                }
            }
            Object[] row = projectAndFilter(s.readElement());
            if (row != null) {
                rowsQueue.offer(row);
            }
        }

        return splits.isEmpty() && projectEntryAndEmit();
    }

    private void splitOnMigration(int splitIndex) {
        assert splitIndex < splits.size();
        Split<F> split = splits.get(splitIndex);
        IndexIterationPointer[] lastPointers = split.getPointers();

        Map<Address, int[]> addressMap = context.partitionAssignment();
        Map<Address, PartitionIdSet> newPartitionDistributions = new HashMap<>();
        addressMap.remove(split.getAddress());

        int partitionCount = split.getPartitions().getPartitionCount();
        PartitionIdSet actualPartitions = new PartitionIdSet(partitionCount, context.processorPartitions());

        // Full split on disjoint set
        addressMap.forEach((address, partitions) -> {
            PartitionIdSet partitionIdSet = new PartitionIdSet(actualPartitions.getPartitionCount());
            for (int partition : partitions) {
                if (split.getPartitions().contains(partition) && !actualPartitions.contains(partition)) {
                    partitionIdSet.add(partition);
                }
            }
            if (!partitionIdSet.isEmpty()) {
                newPartitionDistributions.put(address, partitionIdSet);
            }
        });

        splitsToRemove.offer(splitIndex);
        newPartitionDistributions.put(split.getAddress(), actualPartitions);
        newPartitionDistributions.forEach((address, partitionSet) ->
                splitsToAdd.offer(new Split<>(partitionSet, address, lastPointers))
        );
    }

    private boolean projectEntryAndEmit() {
        if (!rowsQueue.isEmpty()) {
            Object[] item = rowsQueue.poll();
            System.out.println("<- " + item[0]);
            if (!tryEmit(item)) {
                pendingItem = item;
            }
            return false;
        }
        return true;
    }

    private IndexIterationPointer[] filtersToPointers(@Nonnull IndexFilter filter) {
        return IndexIterationPointer.createFromIndexFilter(filter, evalContext);
    }

    private Object[] projectAndFilter(@Nonnull QueryableEntry<?, ?> entry) {
        row.setKeyValue(entry.getKey(), entry.getKeyData(), entry.getValue(), entry.getValueData());

        if (filter != null && TernaryLogic.isNotTrue(filter.evalTop(row, evalContext))) {
            return null;
        }

        Object[] row = new Object[projections.size()];

        for (int j = 0; j < projections.size(); j++) {
            row[j] = evaluate(projections.get(j), this.row, evalContext);
        }
        return row;
    }

    private static final class Split<F extends CompletableFuture<MapFetchIndexOperationResult>> {
        private final PartitionIdSet partitions;
        private final Address address;
        private IndexIterationPointer[] pointers;
        private List<QueryableEntry<?, ?>> currentBatch;
        private int currentBatchPosition;
        private F future;

        Split(PartitionIdSet partitions, Address address, IndexIterationPointer[] pointers) {
            this.partitions = partitions;
            this.address = address;
            this.pointers = pointers;
            this.currentBatch = new ArrayList<>();
            this.currentBatchPosition = 0;
            this.future = null;
        }

        public boolean start(AbstractIndexReader<F, MapFetchIndexOperationResult, QueryableEntry<?, ?>> reader) {
            if (future == null) {
                future = reader.readBatch(address, partitions, pointers);
                return true;
            }
            return false;
        }

        public boolean readBatch(AbstractIndexReader<F, MapFetchIndexOperationResult, QueryableEntry<?, ?>> reader) {
            MapFetchIndexOperationResult result = null;
            currentBatchPosition = 0;
            try {
                result = reader.toBatchResult(future);
                currentBatch = reader.toRecordSet(result);
                pointers = result.getPointers();
                future = null;
                return currentBatch.isEmpty();
            } catch (MissingPartitionException e) {
                throw rethrow(e);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                return true;
            }
        }

        public QueryableEntry<?, ?> readElement() {
            return currentBatch.get(currentBatchPosition++);
        }

        public boolean batchIsDrained() {
            return currentBatchPosition == currentBatch.size();
        }

        public boolean isDone() {
            return future.isDone();
        }

        public PartitionIdSet getPartitions() {
            return partitions;
        }

        public Address getAddress() {
            return address;
        }

        public IndexIterationPointer[] getPointers() {
            return pointers;
        }
    }

    public static final class MapIndexScanProcessorMetaSupplier
            implements ProcessorMetaSupplier, IdentifiedDataSerializable {
        private static final long serialVersionUID = 1L;

        private MapIndexScanMetadata indexScanMetadata;

        public MapIndexScanProcessorMetaSupplier() {
            // No-op.
        }


        public MapIndexScanProcessorMetaSupplier(
                @Nonnull MapIndexScanMetadata indexScanMetadata
        ) {
            this.indexScanMetadata = indexScanMetadata;
        }

        @Override
        @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new MapIndexScanProcessorSupplier(indexScanMetadata);
        }

        @Override
        public int preferredLocalParallelism() {
            return 1;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(indexScanMetadata);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            indexScanMetadata = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return F_ID;
        }

        @Override
        public int getClassId() {
            return IMAP_INDEX_SCAN_PROCESSOR_META_SUPPLIER;
        }
    }

    public static final class MapIndexScanProcessorSupplier
            implements ProcessorSupplier, IdentifiedDataSerializable {
        static final long serialVersionUID = 1L;

        private MapIndexScanMetadata indexScanMetadata;

        private HazelcastInstance hzInstance;
        private ExpressionEvalContext evalContext;
        private InternalSerializationService serializationService;
        private int[] memberPartitions;

        public MapIndexScanProcessorSupplier() {
            // no-op.
        }

        private MapIndexScanProcessorSupplier(@Nonnull MapIndexScanMetadata indexScanMetadata) {
            this.indexScanMetadata = indexScanMetadata;
            this.memberPartitions = new int[0];
        }

        @Override
        public void init(@Nonnull Context context) {
            hzInstance = context.hazelcastInstance();
            serializationService = ((ProcSupplierCtx) context).serializationService();
            memberPartitions = context.partitionAssignment().get(hzInstance.getCluster().getLocalMember().getAddress());
            evalContext = SimpleExpressionEvalContext.from(context);
        }

        @Override
        @Nonnull
        public List<Processor> get(int count) {
            return Arrays.stream(distributeObjects(count, memberPartitions))
                    .map(partitions ->
                            new MapIndexScanP<>(
                                    LocalMapIndexReader.create(hzInstance, serializationService, indexScanMetadata),
                                    hzInstance,
                                    evalContext,
                                    memberPartitions,
                                    indexScanMetadata
                            )
                    ).collect(toList());
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(indexScanMetadata);
            out.writeInt(memberPartitions.length);
            for (int memberPartition : memberPartitions) {
                out.writeInt(memberPartition);
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            indexScanMetadata = in.readObject();
            int len = in.readInt();
            memberPartitions = new int[len];
            for (int i = 0; i < len; ++i) {
                memberPartitions[i] = in.readInt();
            }
        }

        @Override
        public int getFactoryId() {
            return F_ID;
        }

        @Override
        public int getClassId() {
            return IMAP_INDEX_SCAN_PROCESSOR_SUPPLIER;
        }
    }

    static class LocalMapIndexReader extends AbstractIndexReader<
            InternalCompletableFuture<MapFetchIndexOperationResult>,
            MapFetchIndexOperationResult,
            QueryableEntry<?, ?>> {

        private HazelcastInstance hazelcastInstance;
        private String indexName;

        private LocalMapIndexReader(@Nonnull HazelcastInstance hzInstance,
                                    @Nonnull InternalSerializationService serializationService,
                                    @Nonnull MapIndexScanMetadata indexScanMetadata) {
            super(indexScanMetadata.getMapName(), MapFetchIndexOperationResult::getEntries);
            this.hazelcastInstance = hzInstance;
            this.indexName = indexScanMetadata.getIndexName();
            this.serializationService = serializationService;
        }

        public static LocalMapIndexReader create(@Nonnull HazelcastInstance hzInstance,
                                                 @Nonnull InternalSerializationService serializationService,
                                                 @Nonnull MapIndexScanMetadata indexScanMetadata) {
            return new LocalMapIndexReader(hzInstance, serializationService, indexScanMetadata);
        }

        @Nonnull
        public InternalCompletableFuture<MapFetchIndexOperationResult> readBatch(
                Address address,
                PartitionIdSet partitions,
                IndexIterationPointer[] pointers
        ) {
            MapProxyImpl mapProxyImpl = (MapProxyImpl) hazelcastInstance.getMap(objectName);
            MapOperationProvider operationProvider = mapProxyImpl.getOperationProvider();
            Operation op = operationProvider.createFetchIndexOperation(
                    mapProxyImpl.getName(),
                    indexName,
                    pointers,
                    partitions,
                    MAX_FETCH_SIZE
            );
            return mapProxyImpl.getOperationService().invokeOnTarget(mapProxyImpl.getServiceName(), op, address);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(objectName);
            out.writeString(indexName);
            out.writeString(hazelcastInstance.getName());
            out.writeObject(serializationService);
            out.writeObject(toRecordSetFn);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            objectName = in.readString();
            indexName = in.readString();
            hazelcastInstance = getHazelcastInstance(in.readString());
            serializationService = in.readObject();
            toRecordSetFn = in.readObject();
        }
    }

    static MapIndexScanProcessorMetaSupplier readMapIndexSupplier(MapIndexScanMetadata indexScanMetadata) {
        return new MapIndexScanProcessorMetaSupplier(indexScanMetadata);
    }
}
