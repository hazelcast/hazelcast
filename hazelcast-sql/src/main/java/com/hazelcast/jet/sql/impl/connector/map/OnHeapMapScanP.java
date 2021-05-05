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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.exec.scan.KeyValueIterator;
import com.hazelcast.sql.impl.exec.scan.MapScanExecIterator;
import com.hazelcast.sql.impl.exec.scan.MapScanRow;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.predicate.TernaryLogic;
import com.hazelcast.sql.impl.plan.node.MapScanPlanNode;
import com.hazelcast.sql.impl.row.EmptyRow;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.Row;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class OnHeapMapScanP extends AbstractProcessor {
    protected final IMapTraverser traverser;

    public OnHeapMapScanP(
            @Nonnull String mapName,
            @Nonnull MapScanPlanNode node,
            @Nonnull NodeEngine nodeEngine,
            InternalSerializationService serializationService
    ) {
        Map<UUID, PartitionIdSet> partitionMap = QueryUtils.createPartitionMap(
                nodeEngine,
                nodeEngine.getLocalMember().getVersion(),
                false
        );
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapContainer mapContainer = mapService.getMapServiceContext().getMapContainer(mapName);
        traverser = new IMapTraverser(
                mapContainer,
                partitionMap.get(nodeEngine.getClusterService().getLocalMember().getUuid()).iterator(),
                serializationService,
                node.getProjects(),
                node.getFilter()
        );
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        super.init(context);
        traverser.setCtx(SimpleExpressionEvalContext.from(context));
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(traverser);
    }

    static class IMapTraverser implements Traverser<Row> {
        private SimpleExpressionEvalContext ctx;
        protected final List<Integer> projects;
        protected final Expression<Boolean> filter;
        protected KeyValueIterator iteratorExec;
        private MapScanRow row;
        private boolean done;


        IMapTraverser(
                final MapContainer mapContainer,
                Iterator<Integer> localPartitionsIterator,
                InternalSerializationService serializationService,
                List<Integer> projects,
                Expression<Boolean> filter
        ) {
            iteratorExec = new MapScanExecIterator(mapContainer, localPartitionsIterator, serializationService);
            this.projects = projects;
            this.filter = filter;
        }

        @Override
        public Row next() {
            if (!done && iteratorExec.tryAdvance()) {
                Row row = prepareRow(
                        iteratorExec.getKey(),
                        iteratorExec.getKeyData(),
                        iteratorExec.getValue(),
                        iteratorExec.getValueData()
                );
                done = iteratorExec.done();
                return row;
            }
            return null;
        }

        public void setCtx(SimpleExpressionEvalContext ctx) {
            this.ctx = ctx;
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
        protected Row prepareRow(Object rawKey, Data rawKeyData, Object rawValue, Data rawValueData) {
            row.setKeyValue(rawKey, rawKeyData, rawValue, rawValueData);

            // Filter.
            if (filter != null && TernaryLogic.isNotTrue(filter.evalTop(row, ctx))) {
                return null;
            }

            // Project.
            if (projects.size() == 0) {
                return EmptyRow.INSTANCE;
            }

            HeapRow heapRow = new HeapRow(projects.size());

            for (int j = 0; j < projects.size(); j++) {
                Object projectRes = this.row.get(projects.get(j));

                heapRow.set(j, projectRes);
            }

            return heapRow;
        }
    }

}
