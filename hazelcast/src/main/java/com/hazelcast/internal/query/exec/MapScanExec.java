package com.hazelcast.internal.query.exec;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.expression.Expression;
import com.hazelcast.internal.query.expression.Predicate;
import com.hazelcast.internal.query.row.EmptyRowBatch;
import com.hazelcast.internal.query.row.HeapRow;
import com.hazelcast.internal.query.row.KeyValueRow;
import com.hazelcast.internal.query.row.KeyValueRowExtractor;
import com.hazelcast.internal.query.row.Row;
import com.hazelcast.internal.query.row.RowBatch;
import com.hazelcast.internal.query.worker.data.DataWorker;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.util.Clock;
import com.hazelcast.util.collection.PartitionIdSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;

/**
 * Scans the map.
 */
// TODO: Migration support with remote scans.
// TODO: Ticket for index scans
// TODO: Ticket for HD and hot-restart scans
public class MapScanExec extends AbstractExec implements KeyValueRowExtractor {
    /** Map name. */
    private final String mapName;

    /** Partitions to be scanned. */
    private final PartitionIdSet parts;

    /** Projection expressions. */
    private final List<Expression> projections;

    /** Filter. */
    private final Predicate filter;

    // TODO: Abstract out and re-use
    private MapProxyImpl map;
    private MapService mapService;
    private MapServiceContext mapServiceContext;
    private MapContainer mapContainer;
    private Extractors extractors;
    private InternalSerializationService serializationService;

    /** --- STATE MACHINE --- */

    // TODO: To iterator without collection! https://github.com/hazelcast/hazelcast/issues/15228
    private Collection<Row> rows;
    private Iterator<Row> rowsIter;

    // TODO: Batch.
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
        // TODO: Check if map exists.
        map = (MapProxyImpl)ctx.getNodeEngine().getHazelcastInstance().getMap(mapName);

        mapService = map.getNodeEngine().getService(MapService.SERVICE_NAME);
        mapServiceContext = mapService.getMapServiceContext();
        mapContainer = mapServiceContext.getMapContainer(mapName);
        extractors = mapServiceContext.getExtractors(mapName);
        serializationService = (InternalSerializationService)map.getNodeEngine().getSerializationService();

        keyValueRow = new KeyValueRow(this);
    }

    @Override
    public IterationResult advance() {
        if (rows == null) {
            rows = new ArrayList<Row>();

            for (int i = 0; i < parts.getPartitionCount(); i++) {
                if (!parts.contains(i))
                    continue;

                // Per-partition stuff.
                PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(i);

                RecordStore recordStore = partitionContainer.getRecordStore(mapName);

                // TODO: Clock should be global?
                Iterator<Record> iterator = recordStore.loadAwareIterator(Clock.currentTimeMillis(), false);

                while (iterator.hasNext()) {
                    Record record = iterator.next();

                    Data keyData =  record.getKey(); // TODO: Proper conversion for HD (see PartitionScanRunner)
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

        if (KEY_ATTRIBUTE_NAME.value().equals(path)) {
            res = key;
        }
        else if (THIS_ATTRIBUTE_NAME.value().equals(path)) {
            res = val;
        }
        else {
            boolean isKey = path.startsWith(KEY_ATTRIBUTE_NAME.value());

            Object target;

            if (isKey) {
                target = key;
                path = path.substring(KEY_ATTRIBUTE_NAME.value().length() + 1);
            }
            else
                target = val;

            // TODO: Metadata
            res = extractors.extract(target, path, null);
        }

        if (res instanceof HazelcastJsonValue)
            res = Json.parse(res.toString());

        return res;
    }
}
