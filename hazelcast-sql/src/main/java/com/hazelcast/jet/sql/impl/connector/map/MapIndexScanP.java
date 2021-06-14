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
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.internal.iteration.IndexIterationPointer;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.impl.connector.AbstractIndexReader;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation.MapFetchIndexOperationResult;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation.MigrationDetectedException;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation.MissingPartitionException;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.sql.impl.exec.scan.MapScanRow;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.plan.node.IndexSortMetadata;
import com.hazelcast.sql.impl.plan.node.MapIndexScanMetadata;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.Util.distributeObjects;
import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.jet.sql.impl.ExpressionUtil.evaluate;
import static java.util.stream.Collectors.toList;

/*
Description of MapIndexFetchOp:

Behavior of the operation:
- initially it checks that all requested partition are available. If not -> MissingPartitionException
- saves the migrationStamp
- iterates the index starting at pointer[0], then pointer[1] etc.
- if an entry not contained in the partitionIdSet is encountered, it's filtered out
- reads up tp sizeHint. It can be more to finish the current key.
- checks migrationStamp again. If different -> error.
    The error is the same as at the initial check. It doesn't need to say which partitions are missing
- operation returns. Completed pointers are removed from the `newPointers`.
When all pointers are done, returns an empty array in `newPointers`
Note: if there are more than 1 pointers, the output of the operation isn't sorted.
    Multiple pointers come from multiple disjunction predicates, e.g. `a between 1 and 2 or a between 5 or 6`.
    This operation will not have client counterpart for now.


Behavior of the processor
- Converts `IndexFilter` to `IndexIterationPointer[]`. Replaces `ParameterExpression` with `ConstantExpression`
- initially it assumes that all assigned partitions are local. Sends IndexFetchOp with all local partitons
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

/*
EXAMPLE
The examples will use 2 members M0 and M1 and 4 partitions p0, p1, p2.
Each partition contains 3 strings `a-pN, b-pN, c-pN` (`N` is the partition number).
Initial partition assignment is this:
```
M0: p0, p1, p2
M1: p3
```
Let's say we're doing a full index scan for ordering, so the pointer is a range from [-inf, +inf]. The `sizeHint` is 1.
Here's what the processor on M0 will do:
- submit `IndexFetchOp{pointers: [-inf, +inf], partitions: [p0, p1, p2]}`
- response is `{item: a-p0, newPointers: [a-p0, +inf]}
- now p1 migrates away to M1
- processor submits a new `IndexFetchOp{pointers: [a-p0, +inf], partitions: [p0, p1, p2]}`
- response is MPExc
- processor checks partition table and splits the partitionSet into:
  - target M0, partitions: p0, p2
  - target M1, partitions: p1
- processor submits two ops:
  - M0: `IndexFetchOp{pointers: [a-p0, +inf], partitions: [p0, p2]}`
  - M1: `IndexFetchOp{pointers: [a-p0, +inf], partitions: [p1]}`
- both respond:
  - M0: `{item: a-p2, newPointers: [a-p2, +inf]}
  - M1: `{item: a-p1, newPointers: [a-p1, +inf]}
- let's say, now p0 migrates away
- processor sends requests:
  - M0: `IndexFetchOp{pointers: [a-p2, +inf], partitions: [p0, p2]}`
  - M1: `IndexFetchOp{pointers: [a-p1, +inf], partitions: [p1]}`
- responses:
  - M0: MPExc
  - M1: `{item: b-p1, newPointers: [b-p1, +inf]}`
- processor will split the partitionSet again into three parts:
  - target M0, partitions: p2, lastPointers: [a-p2, +inf]
  - target M1, partitions: p1, lastPointers: [b-p1, +inf]
  - target M1, partitions: p0, lastPointers: [a-p2, +inf]
From here on no further splitting is possible, each subset has 1 partition ID.
    If a further migration happens, just the target will be changed.
We're now executing two separate operations against M1, each with a subset of partitions that M1 owns.
That means that the index will be scanned twice, but for each scan we'll skip over the partitions not in the set.
One day we could implement merging of the operations for one target, but not now.
 */

/**
 * Private API, see methods in {@link SourceProcessors}.
 * <p>
 * The number of Hazelcast partitions should be configured to at least
 * {@code localParallelism * clusterSize}, otherwise some processors will
 * have no partitions assigned to them.
 */
public final class MapIndexScanP<F extends CompletableFuture<MapFetchIndexOperationResult>> extends AbstractProcessor {
    // TODO: discuss this number.
    private static final int MAX_FETCH_SIZE = 64;

    private final AbstractIndexReader<F, MapFetchIndexOperationResult, QueryableEntry> reader;
    private final MapContainer mapContainer;
    private final String indexName;
    private final ExpressionEvalContext evalContext;
    private final List<Expression<?>> projections;
    private Context context;

    private final List<Split<F>> splits;
    private final MapScanRow row;
    private Queue<QueryableEntry> queue;
    private Object[] pendingItem;

    private MapIndexScanP(
            @Nonnull AbstractIndexReader<F, MapFetchIndexOperationResult, QueryableEntry> reader,
            @Nonnull HazelcastInstance hazelcastInstance,
            @Nonnull ExpressionEvalContext evalContext,
            @Nonnull int[] partitions,
            MapIndexScanMetadata indexScanMetadata
    ) {
        this.reader = reader;
        MapService mapService = getNodeEngine(hazelcastInstance).getService(MapService.SERVICE_NAME);
        this.mapContainer = mapService.getMapServiceContext().getMapContainer(indexScanMetadata.getMapName());
        this.indexName = indexScanMetadata.getIndexName();
        this.evalContext = evalContext;
        this.projections = indexScanMetadata.getProjections();
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

        if (indexScanMetadata.getIndexSortMetadata() != null) {
            this.queue = new PriorityQueue(produceComparator(indexScanMetadata.getIndexSortMetadata()));
        } else {
            this.queue = new ArrayDeque<>();
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
            return projectEntryAndEmit();
        }

        Queue<Integer> splitsToRemove = new ArrayDeque<>();

        for (int i = 0; i < splits.size(); ++i) {
            Split s = splits.get(i);
            if (s.start(reader)) {
                continue;
            }
            if (s.batchIsDrained()) {
                if (!s.isDone()) {
                    continue;
                } else {
                    if (s.readBatch(reader)) {
                        // No more items to read, remove waisted split.
                        System.out.println();
                        splitsToRemove.offer(i);
                        continue;
                    }
                }
            }
            queue.offer(s.readElement());
        }
        // Remove all splits which ends their job.
        while (!splitsToRemove.isEmpty()) {
            //noinspection SuspiciousMethodCalls
            splits.remove(splitsToRemove.poll());
        }

        return splits.isEmpty() && projectEntryAndEmit();
    }

    private boolean projectEntryAndEmit() {
        if (!queue.isEmpty()) {
            QueryableEntry entry = queue.poll();
            assert entry != null;
            Object[] item = project(entry);
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

    private Object[] project(QueryableEntry entry) {
        row.setKeyValue(entry.getKey(), entry.getKeyData(), entry.getValue(), entry.getValueData());
        Object[] row = new Object[projections.size()];

        for (int j = 0; j < projections.size(); j++) {
            row[j] = evaluate(projections.get(j), this.row, evalContext);
        }
        return row;
    }

    private Comparator<QueryableEntry> produceComparator(IndexSortMetadata metadata) {
        InternalIndex index = mapContainer.getIndexes().getIndex(indexName);
        String[] indexComponents = index.getComponents();
        assert indexComponents.length > 0;
        return (QueryableEntry x, QueryableEntry y) -> {
            if (x.getAttributeValue(indexComponents[0]).hashCode() > y.getAttributeValue(indexComponents[0]).hashCode()) {
                return 1;
            } else {
                return -1;
            }
        };
    }

    private static final class Split<F extends CompletableFuture<MapFetchIndexOperationResult>> {
        private PartitionIdSet partitions;
        private Address target;
        private IndexIterationPointer[] pointers;
        private List<QueryableEntry> currentBatch;
        private int currentBatchPosition;
        private F future;

        Split(PartitionIdSet partitions, Address target, IndexIterationPointer[] pointers) {
            this.partitions = partitions;
            this.target = target;
            this.pointers = pointers;
            this.currentBatch = new ArrayList<>();
            this.currentBatchPosition = 0;
            this.future = null;
        }

        public boolean start(AbstractIndexReader<F, MapFetchIndexOperationResult, QueryableEntry> reader) {
            if (future == null) {
                future = reader.readBatch(partitions, pointers);
                System.out.println("Send request to read next batch");
                return true;
            }
            System.out.println("Batch is actively reading on position: " + currentBatchPosition);
            return false;
        }

        public boolean readBatch(AbstractIndexReader<F, MapFetchIndexOperationResult, QueryableEntry> reader) {
            MapFetchIndexOperationResult result = null;
            currentBatchPosition = 0;
            try {
                result = reader.toBatchResult(future);
                currentBatch = reader.toRecordSet(result);
                pointers = result.getPointers();
                future = null;
                System.out.println("Successfully updated batches.");
                return currentBatch.isEmpty();
            } catch (MigrationDetectedException | MissingPartitionException e) {
                throw rethrow(e);
            } catch (InterruptedException | ExecutionException e) {
//                e.printStackTrace();
                System.err.println(e.getLocalizedMessage());
                return true;
            }
        }

        public QueryableEntry readElement() {
            System.out.println("Try to read next batch element with index " + (currentBatchPosition + 1));
            return currentBatch.get(currentBatchPosition++);
        }

        public boolean batchIsDrained() {
            System.out.println("Batch is drained.");
            return currentBatchPosition == currentBatch.size();
        }

        public boolean isDone() {
            return future.isDone();
        }
    }

    public static final class MapIndexScanProcessorMetaSupplier<F extends CompletableFuture<MapFetchIndexOperationResult>>
            implements ProcessorMetaSupplier {
        private static final long serialVersionUID = 1L;
        private final BiFunctionEx<
                HazelcastInstance,
                InternalSerializationService,
                AbstractIndexReader<F, MapFetchIndexOperationResult, QueryableEntry>> readerSupplier;

        private final MapIndexScanMetadata indexScanMetadata;

        MapIndexScanProcessorMetaSupplier(
                @Nonnull BiFunctionEx<
                        HazelcastInstance,
                        InternalSerializationService,
                        AbstractIndexReader<F, MapFetchIndexOperationResult, QueryableEntry>> readerSupplier,
                @Nonnull MapIndexScanMetadata indexScanMetadata
        ) {
            this.readerSupplier = readerSupplier;
            this.indexScanMetadata = indexScanMetadata;
        }

        @Override
        @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new MapIndexScanProcessorSupplier<>(readerSupplier, indexScanMetadata);
        }

        @Override
        public int preferredLocalParallelism() {
            return 1;
        }
    }

    public static final class MapIndexScanProcessorSupplier<F extends CompletableFuture<MapFetchIndexOperationResult>>
            implements ProcessorSupplier {
        static final long serialVersionUID = 1L;

        private final BiFunction<
                HazelcastInstance,
                InternalSerializationService,
                AbstractIndexReader<F, MapFetchIndexOperationResult, QueryableEntry>> readerSupplier;
        private final MapIndexScanMetadata indexScanMetadata;

        private transient ExpressionEvalContext evalContext;
        private transient int[] memberPartitions;
        private transient HazelcastInstance hzInstance;
        private transient InternalSerializationService serializationService;

        private MapIndexScanProcessorSupplier(
                @Nonnull BiFunctionEx<
                        HazelcastInstance,
                        InternalSerializationService,
                        AbstractIndexReader<F, MapFetchIndexOperationResult, QueryableEntry>> readerSupplier,
                @Nonnull MapIndexScanMetadata indexScanMetadata
        ) {
            this.readerSupplier = readerSupplier;
            this.indexScanMetadata = indexScanMetadata;
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
                                    readerSupplier.apply(hzInstance, serializationService),
                                    hzInstance,
                                    evalContext,
                                    memberPartitions,
                                    indexScanMetadata
                            )
                    ).collect(toList());
        }
    }

    static class LocalMapIndexReader extends AbstractIndexReader<
            InternalCompletableFuture<MapFetchIndexOperationResult>,
            MapFetchIndexOperationResult,
            QueryableEntry> {

        private final HazelcastInstance hazelcastInstance;
        private final MapProxyImpl mapProxyImpl;
        private final String indexName;

        LocalMapIndexReader(@Nonnull HazelcastInstance hzInstance,
                            @Nonnull InternalSerializationService serializationService,
                            @Nonnull MapIndexScanMetadata indexScanMetadata) {
            super(indexScanMetadata.getMapName(), MapFetchIndexOperationResult::getEntries);
            this.hazelcastInstance = hzInstance;
            this.mapProxyImpl = (MapProxyImpl) hzInstance.getMap(indexScanMetadata.getMapName());
            this.indexName = indexScanMetadata.getIndexName();
            this.serializationService = serializationService;
        }

        @Nonnull
        @Override
        public InternalCompletableFuture<MapFetchIndexOperationResult> readBatch(
                PartitionIdSet partitions,
                IndexIterationPointer[] pointers
        ) {
            MapOperationProvider operationProvider = mapProxyImpl.getOperationProvider();
            Address address = hazelcastInstance.getCluster().getLocalMember().getAddress();
            Operation op = operationProvider.createFetchIndexOperation(
                    mapProxyImpl.getName(),
                    indexName,
                    pointers,
                    partitions,
                    MAX_FETCH_SIZE
            );
            return mapProxyImpl.getOperationService().invokeOnTarget(mapProxyImpl.getServiceName(), op, address);
        }

        public InternalCompletableFuture<MapFetchIndexOperationResult> readBatch(
                Address address,
                PartitionIdSet partitions,
                IndexIterationPointer[] pointers
        ) {
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

    }

    static MapIndexScanProcessorMetaSupplier readMapIndexSupplier(MapIndexScanMetadata indexScanMetadata) {
        return new MapIndexScanProcessorMetaSupplier<>((hzInstance, serializationService) ->
                new LocalMapIndexReader(hzInstance, serializationService, indexScanMetadata),
                indexScanMetadata
        );
    }
}
