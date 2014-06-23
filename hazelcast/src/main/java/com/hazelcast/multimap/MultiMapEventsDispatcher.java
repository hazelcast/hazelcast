package com.hazelcast.multimap;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.DataAwareEntryEvent;
import com.hazelcast.map.EntryEventData;
import com.hazelcast.map.EventData;
import com.hazelcast.map.MapEventData;

import java.util.logging.Level;

/**
 * Dispatches multiMapEvents to appropriate methods of given listener
 */
class MultiMapEventsDispatcher {

    private final ILogger logger = Logger.getLogger(MultiMapEventsDispatcher.class);

    private final ClusterService clusterService;
    private final MultiMapService multiMapService;

    public MultiMapEventsDispatcher(MultiMapService multiMapService, ClusterService clusterService) {
        this.multiMapService = multiMapService;
        this.clusterService = clusterService;
    }

    private void incrementEventStats(IMapEvent event) {
        multiMapService.getLocalMultiMapStatsImpl(event.getName()).incrementReceivedEvents();
    }

    public void dispatchEvent(EventData eventData, EntryListener listener) {
        if (eventData instanceof EntryEventData) {
            dispatchEntryEventData(eventData, listener);
        } else if (eventData instanceof MapEventData) {
            dispatchMapEventData(eventData, listener);
        } else {
            throw new IllegalArgumentException("Unknown multimap event data");
        }
    }

    private void dispatchMapEventData(EventData eventData, EntryListener listener) {
        final MapEventData mapEventData = (MapEventData) eventData;
        final Member member = getMemberOrNull(eventData);
        if (member == null) {
            return;
        }
        final MapEvent event = createMapEvent(mapEventData, member);
        dispatch0(event, listener);
        incrementEventStats(event);
    }

    private MapEvent createMapEvent(MapEventData mapEventData, Member member) {
        return new MapEvent(mapEventData.getMapName(), member,
                mapEventData.getEventType(), mapEventData.getNumberOfEntries());
    }

    private void dispatchEntryEventData(EventData eventData, EntryListener listener) {
        final EntryEventData entryEventData = (EntryEventData) eventData;
        final Member member = getMemberOrNull(eventData);

        final EntryEvent event = createDataAwareEntryEvent(entryEventData, member);
        dispatch0(event, listener);
        incrementEventStats(event);
    }

    private Member getMemberOrNull(EventData eventData) {
        final Member member = clusterService.getMember(eventData.getCaller());
        if (member == null) {
            if (logger.isLoggable(Level.INFO)) {
                logger.info("Dropping event " + eventData + " from unknown address:" + eventData.getCaller());
            }
        }
        return member;
    }

    private DataAwareEntryEvent createDataAwareEntryEvent(EntryEventData entryEventData, Member member) {
        return new DataAwareEntryEvent(member, entryEventData.getEventType(), entryEventData.getMapName(),
                entryEventData.getDataKey(), entryEventData.getDataNewValue(),
                entryEventData.getDataOldValue(), multiMapService.getSerializationService());
    }

    private void dispatch0(IMapEvent event, EntryListener listener) {
        switch (event.getEventType()) {
            case ADDED:
                listener.entryAdded((EntryEvent) event);
                break;
            case EVICTED:
                break;
            case UPDATED:
                break;
            case REMOVED:
                listener.entryRemoved((EntryEvent) event);
                break;
            case EVICT_ALL:
                listener.mapEvicted((MapEvent) event);
                break;
            case CLEAR_ALL:
                listener.mapCleared((MapEvent) event);
                break;
            default:
                throw new IllegalArgumentException("Invalid event type: " + event.getEventType());
        }
    }
}
