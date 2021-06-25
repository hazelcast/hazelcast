/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.ComparatorEx;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
Behavior of the processor
- Converts `IndexFilter` to `IndexIterationPointer[]`.
- Replaces `ParameterExpression` with `ConstantExpression`
- initially it assumes that all assigned partitions are local. Sends IndexFetchOp with all local partitions
- When response is received, saves the new `pointers` to `lastPointers` and
  sends another one immediately with the new pointers and then emits to processor's outbox
*/

/**
 * // TODO: [sasha] detailed description
 */
public final class MapIndexScanP<F extends CompletableFuture<MapFetchIndexOperationResult>> extends AbstractProcessor {
    // TODO: [sasha] discuss this number.
    private static final int MAX_FETCH_SIZE = 64;

    private final AbstractIndexReader<F, MapFetchIndexOperationResult, QueryableEntry<?, ?>> reader;
    private final ExpressionEvalContext evalContext;
    private final List<Expression<?>> projection;
    private final List<Expression<?>> fullProjection;
    private final Expression<Boolean> filter;
    private final ComparatorEx<Object[]> comparator;
    private transient Context context;

    private final ArrayList<Split<F>> splits = new ArrayList<>();
    private final MapScanRow row;
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
        this.projection = indexScanMetadata.getProjection();
        this.fullProjection = indexScanMetadata.getFullProjection();
        this.filter = indexScanMetadata.getRemainingFilter();
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
            this.comparator = indexScanMetadata.getComparator();
        } else {
            this.comparator = null;
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

        if (splits.isEmpty()) {
            return true;
        }

        Object[] extreme = null;
        QueryableEntry<?, ?> extremeEntry = null;
        int extremeIndex = -1;

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
                            // No more items to read, remove finished split.
                            splits.remove(i--);
                            continue;
                        }
                    } catch (MissingPartitionException e) {
                        List<Split<F>> newSplits = splitOnMigration(i);
                        splits.remove(i--);
                        splits.addAll(newSplits);
                        continue;
                    }
                }
            }
            QueryableEntry<?, ?> entry = s.readElement();
            Object[] row = transformAndFilter(entry, i);
            if (row != null) {
                if (extreme == null || comparator.compare(row, extreme) > 0) {
                    extreme = row;
                    extremeEntry = entry;
                    extremeIndex = i;
                }
            }
        }

        if (extreme != null) {
            splits.get(extremeIndex).incrementBatchPosition();
        } else {
            // item was filtered or no splits are executing at the moment;
            return false;
        }

        extreme = project(extremeEntry);

        if (!tryEmit(extreme)) {
            pendingItem = extreme;
        }

        return splits.isEmpty();
    }

    /**
     * Perform splitting of split unit after receiving {@code MissingPartitionException}.
     * Method gets current partition table and proceed with following procedure :
     * <p>
     * It splits the partitions assigned to the processor
     * according to the new partition table into disjoint sets,
     * one for each member having some partition assigned to this processor.
     * <p>
     *
     * @param splitIndex index of 'split' unit to make split
     * @return collection of new split units
     */
    List<Split<F>> splitOnMigration(int splitIndex) {
        assert splitIndex < splits.size();
        Split<F> split = splits.get(splitIndex);
        IndexIterationPointer[] lastPointers = split.getPointers();

        List<Split<F>> newSplits = new ArrayList<>();

        Map<Address, int[]> addressMap = context.partitionAssignment();
        Map<Address, PartitionIdSet> newPartitionDistributions = new HashMap<>();

        int partitionCount = split.getPartitions().getPartitionCount();
        PartitionIdSet actualPartitions = new PartitionIdSet(partitionCount, addressMap.get(split.getAddress()));
        PartitionIdSet intersection = split.getPartitions().intersectCopy(actualPartitions);

        addressMap.remove(split.getAddress());

        // Full split on disjoint sets
        addressMap.forEach((address, partitions) -> {
            PartitionIdSet partitionIdSet = new PartitionIdSet(split.getPartitions().getPartitionCount());
            for (int partition : partitions) {
                if (split.getPartitions().contains(partition) && !intersection.contains(partition)) {
                    partitionIdSet.add(partition);
                }
            }
            if (!partitionIdSet.isEmpty()) {
                newPartitionDistributions.put(address, partitionIdSet);
            }
        });

        newPartitionDistributions.put(split.getAddress(), intersection);
        newPartitionDistributions.forEach((address, partitionSet) ->
                newSplits.add(new Split<>(partitionSet, address, lastPointers))
        );
        return newSplits;
    }

    private IndexIterationPointer[] filtersToPointers(@Nonnull IndexFilter filter) {
        return IndexIterationPointer.createFromIndexFilter(filter, evalContext);
    }

    private Object[] transformAndFilter(@Nonnull QueryableEntry<?, ?> entry, int splitIndex) {
        row.setKeyValue(entry.getKey(), entry.getKeyData(), entry.getValue(), entry.getValueData());

        if (filter != null && TernaryLogic.isNotTrue(filter.evalTop(row, evalContext))) {
            assert splitIndex < splits.size();
            // filtered items should be incremented manually after filter check failure.
            splits.get(splitIndex).incrementBatchPosition();
            return null;
        }

        Object[] row = new Object[fullProjection.size()];
        for (int j = 0; j < fullProjection.size(); j++) {
            row[j] = evaluate(fullProjection.get(j), this.row, evalContext);
        }
        return row;
    }

    private Object[] project(@Nonnull QueryableEntry<?, ?> entry) {
        row.setKeyValue(entry.getKey(), entry.getKeyData(), entry.getValue(), entry.getValueData());

        Object[] row = new Object[projection.size()];
        for (int j = 0; j < projection.size(); j++) {
            row[j] = evaluate(projection.get(j), this.row, evalContext);
        }
        return row;
    }

    /**
     * Basic unit of index scan execution bounded to concrete member and partitions set.
     * Can be splitted on different units after migration detection and handling.
     *
     * @param <F> future which contains result of operation execution
     */
    static final class Split<F extends CompletableFuture<MapFetchIndexOperationResult>> {
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
            return currentBatch.get(currentBatchPosition);
        }

        public void incrementBatchPosition() {
            ++currentBatchPosition;
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

    static final class LocalMapIndexReader extends AbstractIndexReader<
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
