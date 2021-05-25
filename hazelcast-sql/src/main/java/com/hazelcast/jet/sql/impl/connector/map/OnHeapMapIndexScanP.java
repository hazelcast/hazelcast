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
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.Partition;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.exec.scan.KeyValueIterator;
import com.hazelcast.sql.impl.exec.scan.MapScanRow;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.MapIndexScanExecIterator;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.predicate.TernaryLogic;
import com.hazelcast.sql.impl.plan.node.MapIndexScanMetadata;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.Util.distributeObjects;
import static com.hazelcast.jet.sql.impl.ExpressionUtil.evaluate;
import static com.hazelcast.jet.sql.impl.JetSqlSerializerHook.F_ID;
import static com.hazelcast.jet.sql.impl.JetSqlSerializerHook.IMAP_INDEX_SCAN_PROCESSOR;
import static com.hazelcast.jet.sql.impl.JetSqlSerializerHook.IMAP_INDEX_SCAN_PROCESSOR_META_SUPPLIER;
import static com.hazelcast.jet.sql.impl.JetSqlSerializerHook.IMAP_INDEX_SCAN_PROCESSOR_SUPPLIER;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

/**
 * This is SQL internal processor to provide a backend for IMap index scan.
 * <p>
 * The main idea here is to encapsulate all map walking process into IMapIndexTraverser.
 * There is a special {@link MapIndexScanExecIterator} inside the traverser that allows
 * it to walk over all map partitions and extract map entries.
 * IMapIndexTraverser does a setup of MapIndexScanExecIterator and starts traversing map partitions.
 * Simultaneously, usage of Traverser interface saves simplicity and cleanliness:
 * it just returns all map entries which it can read.
 */

public final class OnHeapMapIndexScanP extends AbstractProcessor implements IdentifiedDataSerializable {
    private IMapIndexTraverser traverser;
    private MapIndexScanMetadata indexScanMetadata;
    private PartitionIdSet partitions;

    public OnHeapMapIndexScanP() {
        // No-op.
    }

    private OnHeapMapIndexScanP(
            @Nonnull MapIndexScanMetadata indexScanMetadata,
            @Nonnull PartitionIdSet partitions
    ) {
        this.indexScanMetadata = indexScanMetadata;
        this.partitions = partitions;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        NodeEngine nodeEngine = ((HazelcastInstanceImpl) context.jetInstance().getHazelcastInstance()).node.nodeEngine;

        traverser = new IMapIndexTraverser(
                nodeEngine,
                indexScanMetadata,
                this.partitions,
                SimpleExpressionEvalContext.from(context)
        );
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(traverser);
    }

    static class IMapIndexTraverser implements Traverser<Object[]> {
        private final MapContainer mapContainer;
        private final List<Expression<?>> projections;
        private final Expression<Boolean> filter;
        private PartitionIdSet partitions;
        private final MapScanRow row;
        private final SimpleExpressionEvalContext ctx;

        private InternalIndex index;

        private final String indexName;
        private final IndexFilter indexFilter;
        private final List<QueryDataType> converterTypes;
        private final int componentCount;
        // The collation of index entries
        private List<Boolean> ascs;

        /**
         * Stamp to ensure that indexed partitions are stable throughout query execution.
         */
        private Long partitionStamp;

        private final KeyValueIterator iteratorExec;

        IMapIndexTraverser(
                @Nonnull final NodeEngine nodeEngine,
                @Nonnull final MapIndexScanMetadata indexScanMetadata,
                @Nonnull final PartitionIdSet partitions,
                @Nonnull SimpleExpressionEvalContext ctx

        ) {
            this.ctx = ctx;
            MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
            this.mapContainer = mapService.getMapServiceContext().getMapContainer(indexScanMetadata.getMapName());
            this.projections = indexScanMetadata.getProjects();
            this.filter = indexScanMetadata.getFilter();

            this.indexName = indexScanMetadata.getIndexName();
            this.indexFilter = indexScanMetadata.getIndexFilter();
            this.converterTypes = indexScanMetadata.getConverterTypes();
            this.componentCount = indexScanMetadata.getIndexComponentCount();
            this.ascs = indexScanMetadata.getAscs();

            this.iteratorExec = createIterator();
            this.row = MapScanRow.create(
                    indexScanMetadata.getKeyDescriptor(),
                    indexScanMetadata.getValueDescriptor(),
                    indexScanMetadata.getFieldPaths(),
                    indexScanMetadata.getFieldTypes(),
                    mapContainer.getExtractors(),
                    ctx.getSerializationService()
            );
        }

        @Override
        public Object[] next() {
            while (iteratorExec.tryAdvance()) {
                Object[] row = prepareRow(
                        iteratorExec.getKey(),
                        iteratorExec.getKeyData(),
                        iteratorExec.getValue(),
                        iteratorExec.getValueData()
                );
                if (row != null) {
                    return row;
                }
            }
            return null;
        }

        protected KeyValueIterator createIterator() {
            Indexes indexes = mapContainer.getIndexes();

            if (indexes == null) {
                throw QueryException.error(
                        SqlErrorCode.INDEX_INVALID,
                        "Cannot use the index \"" + indexName + "\" of the IMap \""
                                + mapContainer.getName() + "\" because it is not global "
                                + "(make sure the property \"" + ClusterProperty.GLOBAL_HD_INDEX_ENABLED
                                + "\" is set to \"true\")"
                ).markInvalidate();
            }

            index = indexes.getIndex(indexName);

            if (index == null) {
                throw QueryException.error(
                        SqlErrorCode.INDEX_INVALID,
                        "Cannot use the index \"" + indexName + "\" of the IMap \""
                                + mapContainer.getName() + "\" because it doesn't exist"
                ).markInvalidate();
            }

            // Make sure that required partitions are indexed
//            partitionStamp = index.getPartitionStamp(this.partitions);
//
//            if (partitionStamp == GlobalIndexPartitionTracker.STAMP_INVALID) {
//                throw invalidIndexStamp();
//            }

            return new MapIndexScanExecIterator(
                    mapContainer.getName(),
                    index,
                    componentCount,
                    indexFilter,
                    ascs,
                    converterTypes,
                    ctx
            );
        }

        private QueryException invalidIndexStamp() {
            throw QueryException.error(
                    SqlErrorCode.INDEX_INVALID,
                    "Cannot use the index \"" + indexName + "\" of the IMap \"" + mapContainer.getName()
                            + "\" due to concurrent migration, or because index creation is still in progress"
            ).markInvalidate();
        }

        /**
         * Prepare the row for the given key and value:
         * 1) Check filter
         * 2) Extract projections
         *
         * @param rawKey       key as object, might be null
         * @param rawKeyData   key as data, might be null
         * @param rawValue     value as object, might be null
         * @param rawValueData value as data, might be null
         * @return Row that is ready for processing by parent operators or {@code null} if the row hasn't passed the filter.
         */
        private Object[] prepareRow(Object rawKey, Data rawKeyData, Object rawValue, Data rawValueData) {
            this.row.setKeyValue(rawKey, rawKeyData, rawValue, rawValueData);

            // Filter.
            if (filter != null && TernaryLogic.isNotTrue(filter.evalTop(this.row, ctx))) {
                return null;
            }

            Object[] row = new Object[projections.size()];

            for (int j = 0; j < projections.size(); j++) {
                row[j] = evaluate(projections.get(j), this.row, ctx);
            }

            return row;
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(traverser);
        out.writeObject(indexScanMetadata);
        out.writeInt(partitions.size());
        for (Integer partition : partitions) {
            out.writeInt(partition);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.traverser = in.readObject();
        this.indexScanMetadata = in.readObject();
        int partitionsSize = in.readInt();
        for (int i = 0; i < partitionsSize; i++) {
            partitions.add(in.readInt());
        }
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public int getClassId() {
        return IMAP_INDEX_SCAN_PROCESSOR;
    }

    public static ProcessorMetaSupplier onHeapMapIndexScanP(@Nonnull MapIndexScanMetadata indexScanMetadata) {
        return new OnHeapMapIndexScanMetaSupplier(indexScanMetadata);
    }

    public static final class OnHeapMapIndexScanMetaSupplier implements ProcessorMetaSupplier, IdentifiedDataSerializable {
        private MapIndexScanMetadata indexScanMetadata;
        private transient Map<Address, List<Integer>> addrToPartitions;

        public OnHeapMapIndexScanMetaSupplier() {

        }

        private OnHeapMapIndexScanMetaSupplier(@Nonnull MapIndexScanMetadata indexScanMetadata) {
            this.indexScanMetadata = indexScanMetadata;
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            Set<Partition> partitions = context.jetInstance()
                    .getHazelcastInstance()
                    .getPartitionService()
                    .getPartitions();

            addrToPartitions = partitions.stream()
                    .collect(groupingBy(
                            partition -> partition.getOwner().getAddress(),
                            mapping(Partition::getPartitionId, toList()))
                    );
        }

        @Nonnull
        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new OnHeapMapIndexScanSupplier(indexScanMetadata, addrToPartitions.get(address));
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(indexScanMetadata);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.indexScanMetadata = in.readObject();
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

    public static final class OnHeapMapIndexScanSupplier implements ProcessorSupplier, IdentifiedDataSerializable {
        private MapIndexScanMetadata indexScanMetadata;
        private List<Integer> memberPartitions;

        public OnHeapMapIndexScanSupplier() {
        }

        private OnHeapMapIndexScanSupplier(MapIndexScanMetadata indexScanMetadata, List<Integer> memberPartitions) {
            this.indexScanMetadata = indexScanMetadata;
            this.memberPartitions = memberPartitions;
        }

        @Nonnull
        @Override
        public List<Processor> get(int count) {
            return distributeObjects(count, memberPartitions).values().stream()
                    .map(partitions ->
                            new OnHeapMapIndexScanP(indexScanMetadata, new PartitionIdSet(partitions.size(), partitions)))
                    .collect(toList());
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(indexScanMetadata);
            SerializationUtil.writeList(memberPartitions, out);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.indexScanMetadata = in.readObject();
            this.memberPartitions = SerializationUtil.readList(in);
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
}
