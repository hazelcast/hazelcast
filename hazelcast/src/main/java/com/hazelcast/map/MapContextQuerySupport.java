package com.hazelcast.map;

import com.hazelcast.map.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryResultEntryImpl;
import com.hazelcast.util.SortingUtil;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Helper methods for querying map partitions.
 */
public class MapContextQuerySupport {

    private MapServiceContext mapServiceContext;

    public MapContextQuerySupport(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
    }

    public QueryResult queryOnPartition(String mapName, Predicate predicate, int partitionId) {
        final QueryResult result = new QueryResult();
        final PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
        final RecordStore recordStore = container.getRecordStore(mapName);
        final Map<Data, Record> records = recordStore.getReadonlyRecordMapByWaitingMapStoreLoad();
        final SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        final PagingPredicate pagingPredicate = predicate instanceof PagingPredicate ? (PagingPredicate) predicate : null;
        List<QueryEntry> list = new LinkedList<QueryEntry>();
        for (Record record : records.values()) {
            Data key = record.getKey();
            Object value = record.getValue();
            if (value == null) {
                continue;
            }
            QueryEntry queryEntry = new QueryEntry(serializationService, key, key, value);
            if (predicate.apply(queryEntry)) {
                if (pagingPredicate != null) {
                    Map.Entry anchor = pagingPredicate.getAnchor();
                    if (anchor != null
                            && SortingUtil.compare(pagingPredicate.getComparator(),
                            pagingPredicate.getIterationType(), anchor, queryEntry) >= 0) {
                        continue;
                    }
                }
                list.add(queryEntry);
            }
        }
        list = getPage(list, pagingPredicate);
        for (QueryEntry entry : list) {
            result.add(new QueryResultEntryImpl(entry.getKeyData(), entry.getKeyData(), entry.getValueData()));
        }
        return result;
    }

    private List getPage(List<QueryEntry> list, PagingPredicate pagingPredicate) {
        if (pagingPredicate == null) {
            return list;
        }
        final Comparator<Map.Entry> wrapperComparator = SortingUtil.newComparator(pagingPredicate);
        Collections.sort(list, wrapperComparator);
        if (list.size() > pagingPredicate.getPageSize()) {
            list = list.subList(0, pagingPredicate.getPageSize());
        }
        return list;
    }

}
