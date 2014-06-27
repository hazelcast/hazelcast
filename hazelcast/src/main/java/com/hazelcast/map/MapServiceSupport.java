package com.hazelcast.map;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordInfo;
import com.hazelcast.map.record.RecordReplicationInfo;
import com.hazelcast.map.wan.MapReplicationRemove;
import com.hazelcast.map.wan.MapReplicationUpdate;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryResultEntryImpl;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.SortingUtil;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

abstract class MapServiceSupport extends MapEventPublisherSupport {

    private final ConcurrentMap<String, LocalMapStatsImpl> statsMap = new ConcurrentHashMap<String, LocalMapStatsImpl>(1000);

    private final ConstructorFunction<String, LocalMapStatsImpl> localMapStatsConstructorFunction
            = new ConstructorFunction<String, LocalMapStatsImpl>() {
        public LocalMapStatsImpl createNew(String key) {
            return new LocalMapStatsImpl();
        }
    };

    protected MapServiceSupport(NodeEngine nodeEngine) {
        super(nodeEngine);

    }

    public LocalMapStatsImpl getLocalMapStatsImpl(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, localMapStatsConstructorFunction);
    }

    public void applyRecordInfo(Record record, RecordInfo replicationInfo) {
        record.setStatistics(replicationInfo.getStatistics());
        record.setVersion(replicationInfo.getVersion());
        record.setEvictionCriteriaNumber(replicationInfo.getEvictionCriteriaNumber());
        record.setTtl(replicationInfo.getTtl());
        record.setLastAccessTime(replicationInfo.getLastAccessTime());
        record.setLastUpdateTime(replicationInfo.getLastUpdateTime());
    }

    public RecordReplicationInfo createRecordReplicationInfo(Record record) {
        final RecordInfo info = createRecordInfo(record);
        return new RecordReplicationInfo(record.getKey(), toData(record.getValue()), info);
    }

    public RecordInfo createRecordInfo(Record record) {
        final RecordInfo info = new RecordInfo();
        info.setStatistics(record.getStatistics());
        info.setVersion(record.getVersion());
        info.setEvictionCriteriaNumber(record.getEvictionCriteriaNumber());
        info.setLastAccessTime(record.getLastAccessTime());
        info.setLastUpdateTime(record.getLastUpdateTime());
        info.setTtl(record.getTtl());
        return info;
    }

    public Record createRecord(String name, Data key, Object value, long ttl, long now) {
        MapContainer mapContainer = getMapContainer(name);
        Record record = mapContainer.getRecordFactory().newRecord(key, value);
        record.setLastAccessTime(now);
        record.setLastUpdateTime(now);
        record.setCreationTime(now);
        final long ttlFromMapConfig = getTimeToLive(mapContainer);
        if (ttl < 0L && ttlFromMapConfig > 0L) {
            record.setTtl(ttlFromMapConfig);
        } else if (ttl > 0L) {
            record.setTtl(ttl);
        }
        return record;
    }

    public QueryResult queryOnPartition(String mapName, Predicate predicate, int partitionId) {
        final QueryResult result = new QueryResult();
        final PartitionContainer container = getPartitionContainer(partitionId);
        final RecordStore recordStore = container.getRecordStore(mapName);
        final Map<Data, Record> records = recordStore.getReadonlyRecordMapByWaitingMapStoreLoad();
        final SerializationService serializationService = nodeEngine.getSerializationService();
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

    public String addLocalEventListener(EntryListener entryListener, String mapName) {
        EventRegistration registration = nodeEngine.getEventService().
                registerLocalListener(getServiceName(), mapName, entryListener);
        return registration.getId();
    }

    public String addLocalEventListener(EntryListener entryListener, EventFilter eventFilter, String mapName) {
        EventRegistration registration = nodeEngine.getEventService().
                registerLocalListener(getServiceName(), mapName, eventFilter, entryListener);
        return registration.getId();
    }

    public String addEventListener(EntryListener entryListener, EventFilter eventFilter, String mapName) {
        EventRegistration registration = nodeEngine.getEventService().
                registerListener(getServiceName(), mapName, eventFilter, entryListener);
        return registration.getId();
    }

    public boolean removeEventListener(String mapName, String registrationId) {
        return nodeEngine.getEventService().deregisterListener(getServiceName(), mapName, registrationId);
    }

    public void interceptAfterGet(String mapName, Object value) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        if (!interceptors.isEmpty()) {
            value = toObject(value);
            for (MapInterceptor interceptor : interceptors) {
                interceptor.afterGet(value);
            }
        }
    }

    public Object interceptPut(String mapName, Object oldValue, Object newValue) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        Object result = null;
        if (!interceptors.isEmpty()) {
            result = toObject(newValue);
            oldValue = toObject(oldValue);
            for (MapInterceptor interceptor : interceptors) {
                Object temp = interceptor.interceptPut(oldValue, result);
                if (temp != null) {
                    result = temp;
                }
            }
        }
        return result == null ? newValue : result;
    }

    public void interceptAfterPut(String mapName, Object newValue) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        if (!interceptors.isEmpty()) {
            newValue = toObject(newValue);
            for (MapInterceptor interceptor : interceptors) {
                interceptor.afterPut(newValue);
            }
        }
    }

    public Object interceptRemove(String mapName, Object value) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        Object result = null;
        if (!interceptors.isEmpty()) {
            result = toObject(value);
            for (MapInterceptor interceptor : interceptors) {
                Object temp = interceptor.interceptRemove(result);
                if (temp != null) {
                    result = temp;
                }
            }
        }
        return result == null ? value : result;
    }

    public void interceptAfterRemove(String mapName, Object value) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        if (!interceptors.isEmpty()) {
            for (MapInterceptor interceptor : interceptors) {
                value = toObject(value);
                interceptor.afterRemove(value);
            }
        }
    }

    public String addInterceptor(String mapName, MapInterceptor interceptor) {
        return getMapContainer(mapName).addInterceptor(interceptor);
    }

    public void removeInterceptor(String mapName, String id) {
        getMapContainer(mapName).removeInterceptor(id);
    }

    // todo interceptors should get a wrapped object which includes the serialized version
    public Object interceptGet(String mapName, Object value) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        Object result = null;
        if (!interceptors.isEmpty()) {
            result = toObject(value);
            for (MapInterceptor interceptor : interceptors) {
                Object temp = interceptor.interceptGet(result);
                if (temp != null) {
                    result = temp;
                }
            }
        }
        return result == null ? value : result;
    }

    public void publishWanReplicationUpdate(String mapName, EntryView entryView) {
        MapContainer mapContainer = getMapContainer(mapName);
        MapReplicationUpdate replicationEvent = new MapReplicationUpdate(mapName, mapContainer.getWanMergePolicy(),
                entryView);
        mapContainer.getWanReplicationPublisher().publishReplicationEvent(getServiceName(), replicationEvent);
    }

    public void publishWanReplicationRemove(String mapName, Data key, long removeTime) {
        MapContainer mapContainer = getMapContainer(mapName);
        MapReplicationRemove replicationEvent = new MapReplicationRemove(mapName, key, removeTime);
        mapContainer.getWanReplicationPublisher().publishReplicationEvent(getServiceName(), replicationEvent);
    }
}
