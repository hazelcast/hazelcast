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
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.partition.Partition;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.exec.scan.KeyValueIterator;
import com.hazelcast.sql.impl.exec.scan.MapScanExecIterator;
import com.hazelcast.sql.impl.exec.scan.MapScanRow;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.predicate.TernaryLogic;
import com.hazelcast.sql.impl.plan.node.MapScanMetadata;
import com.hazelcast.sql.impl.row.HeapRow;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.Util.distributeObjects;
import static com.hazelcast.jet.sql.impl.ExpressionUtil.evaluate;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

/**
 * This is SQL internal processor to provide a backend for SQL operations (e.g., SELECT * FROM map).
 * <p>
 * The main idea here is to encapsulate all map walking process into IMapTraverser.
 * There is a special {@link MapScanExecIterator} inside the traverser that allows
 * it to walk over all map partitions and extract map entries.
 * IMapTraverser does a setup of MapScanExecIterator and starts traversing map partitions.
 * Simultaneously, usage of Traverser interface saves simplicity and cleanliness:
 * it just returns all map entries which it can read.
 */
public final class OnHeapMapScanP extends AbstractProcessor {
    protected IMapTraverser traverser;
    private final HazelcastInstance hazelcastInstance;
    private final MapScanMetadata mapScanMetadata;
    private final List<Integer> partitions;

    private OnHeapMapScanP(
            @Nonnull HazelcastInstance hazelcastInstance,
            @Nonnull MapScanMetadata mapScanMetadata,
            @Nonnull List<Integer> partitions
    ) {
        this.hazelcastInstance = hazelcastInstance;
        this.mapScanMetadata = mapScanMetadata;
        this.partitions = partitions;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        NodeEngine nodeEngine = ((HazelcastInstanceImpl) hazelcastInstance).node.nodeEngine;

        traverser = new IMapTraverser(
                nodeEngine,
                mapScanMetadata,
                this.partitions,
                SimpleExpressionEvalContext.from(context)
        );
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(traverser);
    }

    static class IMapTraverser implements Traverser<Object[]> {
        private final List<Expression<?>> projections;
        private final Expression<Boolean> filter;
        private final KeyValueIterator iteratorExec;

        private final SimpleExpressionEvalContext ctx;
        private MapScanRow row;

        IMapTraverser(
                @Nonnull final NodeEngine nodeEngine,
                @Nonnull final MapScanMetadata scanMetadata,
                @Nonnull final List<Integer> partitions,
                @Nonnull SimpleExpressionEvalContext ctx

        ) {
            this.ctx = ctx;
            MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
            MapContainer mapContainer = mapService.getMapServiceContext().getMapContainer(scanMetadata.getMapName());
            this.projections = scanMetadata.getProjects();
            this.filter = scanMetadata.getFilter();
            this.iteratorExec = new MapScanExecIterator(mapContainer, partitions.iterator(), ctx.getSerializationService());

            this.row = MapScanRow.create(
                    scanMetadata.getKeyDescriptor(),
                    scanMetadata.getValueDescriptor(),
                    scanMetadata.getFieldPaths(),
                    scanMetadata.getFieldTypes(),
                    mapContainer.getExtractors(),
                    ctx.getSerializationService()
            );

        }

        @Override
        public Object[] next() {
            while (iteratorExec.tryAdvance()) {
                Object[] rows = prepareRow(
                        iteratorExec.getKey(),
                        iteratorExec.getKeyData(),
                        iteratorExec.getValue(),
                        iteratorExec.getValueData()
                );
                if (rows != null) {
                    return rows;
                }
            }
            return null;
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
        protected Object[] prepareRow(Object rawKey, Data rawKeyData, Object rawValue, Data rawValueData) {
            row.setKeyValue(rawKey, rawKeyData, rawValue, rawValueData);

            // Filter.
            if (filter != null && TernaryLogic.isNotTrue(filter.evalTop(row, ctx))) {
                return null;
            }

            HeapRow heapRow = new HeapRow(projections.size());

            for (int j = 0; j < projections.size(); j++) {
                Object projectRes = evaluate(projections.get(j), row, ctx);
                heapRow.set(j, projectRes);
            }

            return heapRow.getValues();
        }
    }

    public static ProcessorMetaSupplier onHeapMapScanP(@Nonnull MapScanMetadata mapScanMetadata) {
        return new OnHeapMapScanMetaSupplier(mapScanMetadata);
    }

    public static final class OnHeapMapScanMetaSupplier implements ProcessorMetaSupplier {
        private final MapScanMetadata mapScanMetadata;
        private transient Map<Address, List<Integer>> addrToPartitions;

        private OnHeapMapScanMetaSupplier(@Nonnull MapScanMetadata mapScanMetadata) {
            this.mapScanMetadata = mapScanMetadata;
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
            return address -> new OnHeapMapScanSupplier(mapScanMetadata, addrToPartitions.get(address));
        }
    }

    public static final class OnHeapMapScanSupplier implements ProcessorSupplier {
        private final MapScanMetadata mapScanMetadata;
        private final List<Integer> memberPartitions;
        private HazelcastInstance hazelcastInstance;

        private OnHeapMapScanSupplier(MapScanMetadata mapScanMetadata, List<Integer> memberPartitions) {
            this.mapScanMetadata = mapScanMetadata;
            this.memberPartitions = memberPartitions;
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            this.hazelcastInstance = context.jetInstance().getHazelcastInstance();
        }

        @Nonnull
        @Override
        public List<Processor> get(int count) {
            return distributeObjects(count, memberPartitions).values().stream()
                    .map(partitions ->
                            new OnHeapMapScanP(hazelcastInstance, mapScanMetadata, partitions))
                    .collect(toList());
        }
    }
}
