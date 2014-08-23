/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;


import com.hazelcast.cache.operation.CacheReplicationOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.EventServiceImpl;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.event.EventType;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Cache Service
 */
public class CacheService implements ManagedService, RemoteService, MigrationAwareService
        , EventPublishingService<CacheEventData, CacheEventListenerAdaptor> {

    public final static String SERVICE_NAME = "hz:impl:cacheService";
    private ILogger logger;
    private NodeEngine nodeEngine;
    private CachePartitionSegment[] segments;
    private ConcurrentMap<String, Map<CacheEntryListenerConfiguration, EventRegistration>> eventRegistrationMap;

    private final ConcurrentMap<String, CacheConfig> configs = new ConcurrentHashMap<String, CacheConfig>();
    private final ConcurrentMap<String, CacheStatistics> statistics = new ConcurrentHashMap<String, CacheStatistics>();

    //region ManagedService
    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        logger = nodeEngine.getLogger(getClass());
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        segments = new CachePartitionSegment[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            segments[i] = new CachePartitionSegment(nodeEngine, this, i);
        }
        eventRegistrationMap = new ConcurrentHashMap<String, Map<CacheEntryListenerConfiguration, EventRegistration>>();
    }

    @Override
    public void reset() {
        final CachePartitionSegment[] partitionSegments = segments;
        for (CachePartitionSegment partitionSegment : partitionSegments) {
            if (partitionSegment != null) {
                partitionSegment.clear();
            }
        }
//        for (NearCache nearCache : nearCacheMap.values()) {
//            nearCache.clear();
//        }

    }

    @Override
    public void shutdown(boolean terminate) {
        if (!terminate) {
            //flushWriteCacheBeforeShutdown();
            //destroyMapStores();
            this.reset();
        }
    }
    //endregion


    //region RemoteService
    @Override
    public DistributedObject createDistributedObject(String objectName) {
        return new CacheDistributedObject(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        for (CachePartitionSegment segment : segments) {
            segment.deleteCache(objectName);
        }
        unregisterAllCacheEntryListener(objectName);
        enableStatistics(objectName,false);
        enableManagement(objectName,false);

        deleteCacheConfig(objectName);
        deleteCacheStat(objectName);
    }
    //endregion


    //region MigrationAwareService
    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        CachePartitionSegment segment = segments[event.getPartitionId()];
        CacheReplicationOperation op = new CacheReplicationOperation(segment, event.getReplicaIndex());
        return op.isEmpty() ? null : op;
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {/*empty*/}

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearPartitionReplica(event.getPartitionId());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearPartitionReplica(event.getPartitionId());
        }
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        segments[partitionId].clear();
    }
    //endregion


    //region CacheService Impls

    public ICacheRecordStore getOrCreateCache(String name, int partitionId) {
        return segments[partitionId].getOrCreateCache(name);
    }

    public ICacheRecordStore getCache(String name, int partitionId) {
        return segments[partitionId].getCache(name);
    }

    public boolean createCacheConfigIfAbsent(CacheConfig config){
        final CacheConfig _config = configs.putIfAbsent(config.getNameWithPrefix(), config);
        return _config == null;
    }

    public boolean updateCacheConfig(CacheConfig config){
        final CacheConfig oldConfig = configs.put(config.getNameWithPrefix(), config);
        return oldConfig != null;
    }

    public void deleteCacheConfig(String name){
        configs.remove(name);
    }

    public CacheStatistics createCacheStatIfAbsent(String name){
        if(!statistics.containsKey(name)){
            statistics.putIfAbsent(name, new CacheStatistics());
        }
        return statistics.get(name);
    }

    public void deleteCacheStat(String name){
        statistics.remove(name);
    }

    public void enableStatistics(String cacheNameWithPrefix, boolean enabled) {
        final CacheConfig cacheConfig = configs.get(cacheNameWithPrefix);
        if( cacheConfig != null){
            cacheConfig.setStatisticsEnabled(enabled);
            if (enabled) {
                final CacheStatistics cacheStatistics = createCacheStatIfAbsent(cacheNameWithPrefix);
                final CacheStatisticsMXBeanImpl mxBean = new CacheStatisticsMXBeanImpl(cacheStatistics);

                MXBeanUtil.registerCacheObject(mxBean, cacheConfig.getUriString(), cacheConfig.getName(), true);
            } else {
                MXBeanUtil.unregisterCacheObject(cacheConfig.getUriString(), cacheConfig.getName(), true);
                deleteCacheStat(cacheNameWithPrefix);
            }
        }
    }

    public void enableManagement(String cacheNameWithPrefix, boolean enabled) {
        final CacheConfig cacheConfig = configs.get(cacheNameWithPrefix);
        if( cacheConfig != null){
            cacheConfig.setManagementEnabled(enabled);
            if (enabled) {
                final CacheMXBeanImpl mxBean = new CacheMXBeanImpl(cacheConfig);
                MXBeanUtil.registerCacheObject(mxBean, cacheConfig.getUriString(), cacheConfig.getName(), false);
            } else {
                MXBeanUtil.unregisterCacheObject(cacheConfig.getUriString(), cacheConfig.getName(), false);
                deleteCacheStat(cacheNameWithPrefix);
            }
        }
    }

    public CacheConfig getCacheConfig(String name) {
        return configs.get(name);
    }

    public Iterable<String> getCacheNames(){
        return configs.keySet();
    }

    public Collection<CacheConfig> getCacheConfigs(){
        return configs.values();
    }

    public Object toObject(Object data) {
        if (data == null)
            return null;
        if (data instanceof Data) {
            return nodeEngine.toObject(data);
        } else {
            return data;
        }
    }

    public Data toData(Object object) {
        if (object == null)
            return null;
        if (object instanceof Data) {
            return (Data) object;
        } else {
            return nodeEngine.getSerializationService().toData(object);
        }
    }

    public void publishEvent(String cacheName, EventType eventType, Data dataKey, Object oldValue, Object value) {
        //TODO clean up this event publish thing
        EventService eventService = getNodeEngine().getEventService();
        Collection<EventRegistration> candidates = eventService.getRegistrations(CacheService.SERVICE_NAME, cacheName);

        if (candidates.isEmpty()) {
            return;
        }
        ArrayList<CacheEventListenerAdaptor> syncListWithOldValue= new ArrayList<CacheEventListenerAdaptor>();
        ArrayList<CacheEventListenerAdaptor> syncListWithoutOldValue= new ArrayList<CacheEventListenerAdaptor>();
        Set<EventRegistration> registrationsWithOldValue = new HashSet<EventRegistration>();
        Set<EventRegistration> registrationsWithoutOldValue = new HashSet<EventRegistration>();
        Object objectValue = toObject(value);
        Object objectOldValue = toObject(oldValue);
        for (EventRegistration candidate : candidates) {
            EventFilter filter = candidate.getFilter();

            final Object key = toObject(dataKey);
            if (filter instanceof CacheEventFilterAdaptor) {
                final CacheEventFilterAdaptor<Object, Object> ceFilter = (CacheEventFilterAdaptor<Object, Object>) filter;
                if (ceFilter.filterEventData(eventType, key, objectValue, objectOldValue)) {
                    if(ceFilter.isSynchronous()){
                        final Object listener = ((EventServiceImpl.Registration) candidate).getListener();
                        if (ceFilter.isOldValueRequired()) {
                            syncListWithOldValue.add((CacheEventListenerAdaptor) listener);
                        } else {
                            syncListWithoutOldValue.add((CacheEventListenerAdaptor) listener);
                        }
                    } else {
                        if (ceFilter.isOldValueRequired()) {
                            registrationsWithOldValue.add(candidate);
                        } else {
                            registrationsWithoutOldValue.add(candidate);
                        }
                    }
                }
            }
        }
        if (registrationsWithOldValue.isEmpty() && registrationsWithoutOldValue.isEmpty()
                && syncListWithOldValue.isEmpty()&& syncListWithoutOldValue.isEmpty()) {
            return;
        }
        Data dataValue = toData(value);
        Data dataOldValue = toData(oldValue);
        if (eventType == EventType.REMOVED || eventType == EventType.EXPIRED) {
            dataValue = dataValue != null ? dataValue : dataOldValue;
        }
//        final Address caller=null;
        int orderKey = dataKey.hashCode();
        CacheEventData eventWithOldValue = new CacheEventData(cacheName, dataKey, dataValue, dataOldValue, eventType);
        CacheEventData eventWithOutOldValue = new CacheEventData(cacheName, dataKey, dataValue, null, eventType);

        nodeEngine.getEventService().publishEvent(SERVICE_NAME, registrationsWithOldValue, eventWithOldValue, orderKey);
        nodeEngine.getEventService().publishEvent(SERVICE_NAME, registrationsWithoutOldValue, eventWithOutOldValue, orderKey);

        //EXECUTE SYNC LISTENERs
        for(CacheEventListenerAdaptor listener:syncListWithOldValue){
            dispatchEvent(eventWithOldValue,listener);
        }
        for(CacheEventListenerAdaptor listener:syncListWithoutOldValue){
            dispatchEvent(eventWithOutOldValue,listener);
        }
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public void dispatchEvent(CacheEventData eventData, CacheEventListenerAdaptor listener) {
        //Member member = nodeEngine.getClusterService().getMember(eventData.getCaller());
        final EventType eventType = eventData.getEventType();
        final Object key = toObject(eventData.getDataKey());
        final Object newValue = toObject(eventData.getDataNewValue());
        final Object oldValue = toObject(eventData.getDataOldValue());
        listener.handleEvent(nodeEngine, eventData.getName(), eventType, key, newValue, oldValue);
    }

    public <K, V> void registerCacheEntryListener(String name, ICache<K, V> source, CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        final CacheEventFilterAdaptor<K, V> eventFilter = new CacheEventFilterAdaptor<K, V>(source, cacheEntryListenerConfiguration);
        final CacheEventListenerAdaptor<K, V> entryListener = new CacheEventListenerAdaptor<K, V>(source, cacheEntryListenerConfiguration);
        final EventService eventService = getNodeEngine().getEventService();
        final EventRegistration registration = eventService.registerListener(CacheService.SERVICE_NAME, name, eventFilter, entryListener);
        Map<CacheEntryListenerConfiguration, EventRegistration> map = eventRegistrationMap.get(name);
        if(map == null){
            map = new HashMap<CacheEntryListenerConfiguration, EventRegistration>();
            eventRegistrationMap.put(name, map);
        }
        map.put(cacheEntryListenerConfiguration, registration);
    }

    public <K, V> void unregisterCacheEntryListener(String name, CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        Map<CacheEntryListenerConfiguration, EventRegistration> map = eventRegistrationMap.get(name);
        if(map != null){
            final EventRegistration eventRegistration = map.remove(cacheEntryListenerConfiguration);
            if (eventRegistration != null) {
                final EventService eventService = getNodeEngine().getEventService();
                eventService.deregisterListener(SERVICE_NAME, name, eventRegistration.getId());
            }
        }
    }

    public void unregisterAllCacheEntryListener(String name) {
        Map<CacheEntryListenerConfiguration, EventRegistration> map = eventRegistrationMap.remove(name);
        if(map != null){
            final EventService eventService = getNodeEngine().getEventService();
            for(EventRegistration eventRegistration:map.values()){
                eventService.deregisterListener(SERVICE_NAME, name, eventRegistration.getId());

                //try to close the listener
                if (((EventServiceImpl.Registration)eventRegistration).getListener() instanceof Closeable) {
                    try {
                        ((Closeable) eventRegistration).close();
                    } catch (IOException e) {
                        //log
                    }
                }
            }
        }
    }



    //endregion


}
