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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.sql.impl.worker.data.DataWorker;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.Predicate;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.KeyValueRow;
import com.hazelcast.sql.impl.row.KeyValueRowExtractor;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.util.Clock;
import com.hazelcast.util.collection.PartitionIdSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;

/**
 * Executor for map scan.
 */
// TODO TODO: Migration support with remote scans.
// TODO TODO: Ticket for index scans
// TODO TODO: Ticket for HD and hot-restart scans
public class MapScanExec extends AbstractExec implements KeyValueRowExtractor {
    /** Map name. */
    private final String mapName;

    /** Partitions to be scanned. */
    private final PartitionIdSet parts;

    /** Projection expressions. */
    private final List<Expression> projections;

    /** Filter. */
    private final Predicate filter;

    /** Map service context. */
    private MapServiceContext mapServiceContext;

    /** Extractors. */
    private Extractors extractors;

    /** Serialization service. */
    private InternalSerializationService serializationService;

    // TODO TODO: To iterator without collection! https://github.com/hazelcast/hazelcast/issues/15228
    /** All rows fetched on first access. */
    private Collection<Row> rows;

    /** Iterator over rows. */
    private Iterator<Row> rowsIter;

    // TODO TODO: To batched implementation to minimize number of virtual calls?
    /** Current row. */
    private Row currentRow;

    /** Row to get data with extractors. */
    private KeyValueRow keyValueRow;

    public MapScanExec(String mapName, PartitionIdSet parts, List<Expression> expressions, Predicate filter) {
        this.mapName = mapName;
        this.parts = parts;
        this.projections = expressions;
        this.filter = filter;
    }

    @Override
    protected void setup0(QueryContext ctx, DataWorker worker) {
        // TODO TODO: Check if map exists.
        MapProxyImpl map = (MapProxyImpl)ctx.getNodeEngine().getHazelcastInstance().getMap(mapName);
        MapService mapService = map.getNodeEngine().getService(MapService.SERVICE_NAME);

        mapServiceContext = mapService.getMapServiceContext();

        extractors = mapServiceContext.getExtractors(mapName);
        serializationService = (InternalSerializationService)map.getNodeEngine().getSerializationService();

        keyValueRow = new KeyValueRow(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public IterationResult advance() {
        if (rows == null) {
            rows = new ArrayList<>();

            for (int i = 0; i < parts.getPartitionCount(); i++) {
                if (!parts.contains(i))
                    continue;

                // Per-partition stuff.
                PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(i);

                RecordStore recordStore = partitionContainer.getRecordStore(mapName);

                // TODO TODO: Clock should be global?
                Iterator<Record> iterator = recordStore.loadAwareIterator(Clock.currentTimeMillis(), false);

                while (iterator.hasNext()) {
                    Record record = iterator.next();

                    Data keyData =  record.getKey(); // TODO TODO: Proper conversion for HD (see PartitionScanRunner)
                    Object valData = record.getValue();

                    Object key = serializationService.toObject(keyData);
                    Object val = valData instanceof Data ? serializationService.toObject(valData) : valData;

                    keyValueRow.setKeyValue(key, val);

                    // Evaluate the filter.
                    if (filter != null && !filter.eval(ctx, keyValueRow))
                        continue;

                    // Create final row.
                    HeapRow row = new HeapRow(projections.size());

                    for (int j = 0; j < projections.size(); j++) {
                        Object projectionRes = projections.get(j).eval(ctx, keyValueRow);

                        row.set(j, projectionRes);
                    }

                    rows.add(row);
                }
            }

            rowsIter = rows.iterator();
        }

        if (rowsIter.hasNext()) {
            currentRow = rowsIter.next();

            return IterationResult.FETCHED;
        }
        else {
            currentRow = null;

            return IterationResult.FETCHED_DONE;
        }
    }

    @Override
    public RowBatch currentBatch() {
        return currentRow != null ? currentRow : EmptyRowBatch.INSTANCE;
    }

    @Override
    public Object extract(Object key, Object val, String path) {
        Object res;

        if (KEY_ATTRIBUTE_NAME.value().equals(path))
            res = key;
        else if (THIS_ATTRIBUTE_NAME.value().equals(path))
            res = val;
        else {
            boolean isKey = path.startsWith(KEY_ATTRIBUTE_NAME.value());

            Object target;

            if (isKey) {
                target = key;
                path = path.substring(KEY_ATTRIBUTE_NAME.value().length() + 1);
            }
            else
                target = val;

            // TODO TODO: What to do with metadata here (last argument)?
            res = extractors.extract(target, path, null);
        }

        if (res instanceof HazelcastJsonValue)
            res = Json.parse(res.toString());

        return res;
    }
}
