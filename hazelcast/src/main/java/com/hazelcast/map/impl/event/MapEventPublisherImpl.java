/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.event;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapPartitionLostEventFilter;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.query.QueryEventFilter;
import com.hazelcast.map.impl.wan.WanMapAddOrUpdateEvent;
import com.hazelcast.map.impl.wan.WanMapEntryView;
import com.hazelcast.map.impl.wan.WanMapRemoveEvent;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.wan.WanPublisher;
import com.hazelcast.wan.impl.DelegatingWanScheme;
import com.hazelcast.wan.impl.InternalWanEvent;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

import static com.hazelcast.internal.util.CollectionUtil.isEmpty;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.event.AbstractFilteringStrategy.FILTER_DOES_NOT_MATCH;

public class MapEventPublisherImpl implements MapEventPublisher {

    /**
     * When {@code true}, enables processing of entry events for
     * listeners with predicates to fit with "query cache" concept: for
     * example when the original event indicates an update from an old
     * value that does not match the predicate to a new value that does
     * match, then the entry listener will be notified with an ADDED
     * event. This affects only map listeners with predicates and the
     * way entry updates are handled. Put/remove operations are not
     * affected, neither are listeners without predicates. Default value
     * is {@code false}, to maintain compatible behavior with previous
     * Hazelcast versions.
     */
    public static final String PROP_LISTENER_WITH_PREDICATE_PRODUCES_NATURAL_EVENT_TYPES
            = "hazelcast.map.entry.filtering.natural.event.types";
    public static final HazelcastProperty LISTENER_WITH_PREDICATE_PRODUCES_NATURAL_EVENT_TYPES
            = new HazelcastProperty(PROP_LISTENER_WITH_PREDICATE_PRODUCES_NATURAL_EVENT_TYPES, false);

    protected final NodeEngine nodeEngine;
    protected final EventService eventService;
    protected final IPartitionService partitionService;
    protected final MapServiceContext mapServiceContext;
    protected final FilteringStrategy filteringStrategy;
    protected final InternalSerializationService serializationService;
    protected final QueryCacheEventPublisher queryCacheEventPublisher;
    protected final ILogger logger;

    public MapEventPublisherImpl(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.logger = nodeEngine.getLogger(MapEventPublisherImpl.class);
        this.partitionService = nodeEngine.getPartitionService();
        this.serializationService = ((InternalSerializationService) nodeEngine.getSerializationService());
        this.eventService = nodeEngine.getEventService();
        if (this.nodeEngine.getProperties().
                getBoolean(LISTENER_WITH_PREDICATE_PRODUCES_NATURAL_EVENT_TYPES)) {
            this.filteringStrategy = new QueryCacheNaturalFilteringStrategy(serializationService, mapServiceContext);
        } else {
            this.filteringStrategy = new DefaultEntryEventFilteringStrategy(serializationService, mapServiceContext);
        }
        this.queryCacheEventPublisher = new QueryCacheEventPublisher(filteringStrategy,
                mapServiceContext.getQueryCacheContext(), serializationService);
    }

    @Override
    public void publishWanUpdate(@Nonnull String mapName,
                                 @Nonnull WanMapEntryView<Object, Object> entryView,
                                 boolean hasLoadProvenance) {
        if (!isOwnedPartition(entryView.getDataKey())) {
            return;
        }

        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        SplitBrainMergePolicy wanMergePolicy = mapContainer.getWanMergePolicy();
        WanMapAddOrUpdateEvent event = new WanMapAddOrUpdateEvent(mapName, wanMergePolicy, entryView);
        publishWanEvent(mapName, event);
    }

    @Override
    public void publishWanRemove(@Nonnull String mapName, @Nonnull Data key) {
        if (!isOwnedPartition(key)) {
            return;
        }

        WanMapRemoveEvent event = new WanMapRemoveEvent(mapName, key, serializationService);
        publishWanEvent(mapName, event);
    }

    /**
     * Publishes the {@code event} to the {@link WanPublisher} configured for this map.
     *
     * @param mapName the map name
     * @param event   the event
     */
    protected void publishWanEvent(String mapName, InternalWanEvent event) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        DelegatingWanScheme wanReplicationPublisher
                = mapContainer.getWanReplicationDelegate();
        if (isOwnedPartition(event.getKey())) {
            wanReplicationPublisher.publishReplicationEvent(event);
        } else {
            wanReplicationPublisher.publishReplicationEventBackup(event);
        }
    }

    private boolean isOwnedPartition(Data dataKey) {
        int partitionId = partitionService.getPartitionId(dataKey);
        return partitionService.getPartition(partitionId, false).isLocal();
    }

    @Override
    public void publishMapEvent(Address caller, String mapName, EntryEventType eventType,
                                int numberOfEntriesAffected) {
        Collection<EventRegistration> mapsListenerRegistrations = getRegistrations(mapName);
        if (isEmpty(mapsListenerRegistrations)) {
            return;
        }

        Collection<EventRegistration> registrations = null;
        for (EventRegistration registration : mapsListenerRegistrations) {
            EventFilter filter = registration.getFilter();

            if (filter instanceof EventListenerFilter) {
                if (!filter.eval(eventType.getType())) {
                    continue;
                }
            }

            if (!(filter instanceof MapPartitionLostEventFilter)) {
                if (registrations == null) {
                    registrations = new ArrayList<>();
                }
                registrations.add(registration);
            }
        }

        if (isEmpty(registrations)) {
            return;
        }

        String source = getThisNodesAddress();
        MapEventData mapEventData = new MapEventData(source, mapName, caller,
                eventType.getType(), numberOfEntriesAffected);
        publishEventInternal(registrations, mapEventData, mapName.hashCode());
    }

    @Override
    public void publishEvent(Address caller, String mapName, EntryEventType eventType,
                             Data dataKey, Object oldValue, Object dataValue) {
        publishEvent(caller, mapName, eventType, dataKey, oldValue, dataValue, null);
    }


    @Override
    public void publishEvent(Address caller, String mapName, EntryEventType eventType,
                             Data dataKey, Object oldValue, Object value, Object mergingValue) {
        Collection<EventRegistration> registrations = getRegistrations(mapName);
        if (isEmpty(registrations)) {
            return;
        }

        publishEvent(registrations, caller, mapName, eventType, dataKey, oldValue, value, mergingValue);
    }

    /**
     * Publish the event to the specified listener {@code registrations} if
     * the event passes the filters specified by the {@link FilteringStrategy}.
     * <p>
     * The method uses the hashcode of the {@code dataKey} to order the
     * events in the event subsystem. This means that all events for the same
     * key will be ordered. Events with different keys need not be ordered.
     *
     * @param registrations the listener registrations to which we are publishing
     * @param caller        the address of the caller that caused the event
     * @param mapName       the map name
     * @param eventType     the event type
     * @param dataKey       the key of the event map entry
     * @param oldValue      the old value of the map entry
     * @param newValue      the new value of the map entry
     * @param mergingValue  the value used when performing a merge
     *                      operation in case of a {@link EntryEventType#MERGED} event.
     *                      This value together with the old value produced the new value.
     */
    private void publishEvent(Collection<EventRegistration> registrations, Address caller, String mapName,
                              EntryEventType eventType, Data dataKey, Object oldValue, Object newValue,
                              Object mergingValue) {
        EntryEventDataCache eventDataCache = filteringStrategy.getEntryEventDataCache();

        int orderKey = pickOrderKey(dataKey);

        for (EventRegistration registration : registrations) {
            publishEventQuietly(caller, mapName, eventType, dataKey, oldValue, newValue,
                    mergingValue, eventDataCache, orderKey, registration);
        }

        // if events were generated, execute the post-publish hook on each one
        if (!eventDataCache.isEmpty()) {
            postPublishEvent(eventDataCache.eventDataIncludingValues(), eventDataCache.eventDataExcludingValues());
        }
    }

    @SuppressWarnings("checkstyle:parameternumber")
    private void publishEventQuietly(Address caller, String mapName, EntryEventType eventType, Data dataKey, Object oldValue,
                                     Object newValue, Object mergingValue, EntryEventDataCache eventDataCache, int orderKey,
                                     EventRegistration registration) {
        try {
            EventFilter filter = registration.getFilter();
            // a filtering strategy determines whether the event must be published on the specific
            // event registration and may alter the type of event to be published
            int eventTypeForPublishing = filteringStrategy.doFilter(filter, dataKey, oldValue, newValue, eventType, mapName);
            if (eventTypeForPublishing == FILTER_DOES_NOT_MATCH) {
                return;
            }

            EntryEventData eventDataToBePublished = eventDataCache
                    .getOrCreateEventData(mapName, caller, dataKey, newValue, oldValue, mergingValue, eventTypeForPublishing,
                            isIncludeValue(filter));
            eventService.publishEvent(SERVICE_NAME, registration, eventDataToBePublished, orderKey);
        } catch (Exception ex) {
            logger.warning("Event publication error for registration: " + registration, ex);
        }
    }

    /**
     * Hook for actions to perform after any of {@link #publishEvent}
     * methods is executed and if there were any registrations for
     * the event. This method will be invoked once per unique {@link
     * EntryEventData} generated by {@code publishEvent}, regardless
     * of the number of registrations on which the event is published.
     *
     * @param eventDataIncludingValues the event data including all of the entry values (old, new, merging)
     * @param eventDataExcludingValues the event data without entry values
     */
    protected void postPublishEvent(Collection<EntryEventData> eventDataIncludingValues,
                                    Collection<EntryEventData> eventDataExcludingValues) {
        // publish event data of interest to query caches; since query cache listener registrations
        // include values (as these are required to properly filter according to the query cache's predicate),
        // we do not take into account eventDataExcludingValues, if any were generated
        if (eventDataIncludingValues != null) {
            for (EntryEventData entryEventData : eventDataIncludingValues) {
                queryCacheEventPublisher.addEventToQueryCache(entryEventData);
            }
        }
    }

    /**
     * Return {@code true} if the {@code filter} requires the entry
     * values (old, new, merging) to be included in the event.
     *
     * @throws IllegalArgumentException if the filter type is not known
     */
    static boolean isIncludeValue(EventFilter filter) {
        // the order of the following ifs is important!
        // QueryEventFilter is instance of EntryEventFilter
        if (filter instanceof EventListenerFilter) {
            filter = ((EventListenerFilter) filter).getEventFilter();
        }
        if (filter instanceof TrueEventFilter) {
            return true;
        }
        if (filter instanceof QueryEventFilter) {
            return ((QueryEventFilter) filter).isIncludeValue();
        }
        if (filter instanceof EntryEventFilter) {
            return ((EntryEventFilter) filter).isIncludeValue();
        }
        throw new IllegalArgumentException("Unknown EventFilter type = [" + filter.getClass().getCanonicalName() + "]");
    }

    @Override
    public void publishMapPartitionLostEvent(Address caller, String mapName, int partitionId) {
        Collection<EventRegistration> registrations = new LinkedList<>();
        for (EventRegistration registration : getRegistrations(mapName)) {
            if (registration.getFilter() instanceof MapPartitionLostEventFilter) {
                registrations.add(registration);
            }
        }

        if (registrations.isEmpty()) {
            return;
        }

        String thisNodesAddress = getThisNodesAddress();
        MapPartitionEventData eventData = new MapPartitionEventData(thisNodesAddress, mapName, caller, partitionId);
        publishEventInternal(registrations, eventData, partitionId);
    }

    @Override
    public void hintMapEvent(Address caller, String mapName, EntryEventType eventType,
                             int numberOfEntriesAffected, int partitionId) {
        queryCacheEventPublisher.hintMapEvent(caller, mapName, eventType, numberOfEntriesAffected, partitionId);
    }

    public void addEventToQueryCache(Object eventData) {
        queryCacheEventPublisher.addEventToQueryCache(eventData);
    }

    @Override
    public boolean hasEventListener(String mapName) {
        return eventService.hasEventRegistration(SERVICE_NAME, mapName);
    }

    /**
     * Return all listener registrations for the map with {@code mapName}.
     *
     * @param mapName the map name
     * @return the collection of listener registrations
     */
    protected Collection<EventRegistration> getRegistrations(String mapName) {
        return eventService.getRegistrations(SERVICE_NAME, mapName);
    }

    /**
     * Returns the hashCode of the {@code key} or -1 if {@code null}
     */
    private int pickOrderKey(Data key) {
        return key == null ? -1 : key.hashCode();
    }

    protected void publishEventInternal(Collection<EventRegistration> registrations, Object eventData, int orderKey) {
        eventService.publishEvent(SERVICE_NAME, registrations, eventData, orderKey);
        queryCacheEventPublisher.addEventToQueryCache(eventData);
    }

    private String getThisNodesAddress() {
        Address thisAddress = nodeEngine.getThisAddress();
        return thisAddress.toString();
    }
}
