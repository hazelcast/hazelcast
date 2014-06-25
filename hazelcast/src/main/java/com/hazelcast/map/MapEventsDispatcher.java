package com.hazelcast.map;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.logging.Level;

/**
 * Dispatches mapEvents to appropriate methods of given listener
 */
class MapEventsDispatcher {

    private final ILogger logger = Logger.getLogger(MapEventsDispatcher.class);

    private final ClusterService clusterService;
    private final MapService mapService;

    public MapEventsDispatcher(MapService mapService, ClusterService clusterService) {
        this.mapService = mapService;
        this.clusterService = clusterService;
    }

    private void incrementEventStats(IMapEvent event) {
        final String mapName = event.getName();
        MapContainer mapContainer = mapService.getMapContainer(mapName);
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            mapService.getLocalMapStatsImpl(mapName).incrementReceivedEvents();
        }
    }

    public void dispatchEvent(EventData eventData, EntryListener listener) {
        if (eventData instanceof EntryEventData) {
            dispatchEntryEventData(eventData, listener);
        } else if (eventData instanceof MapEventData) {
            dispatchMapEventData(eventData, listener);
        } else {
            throw new IllegalArgumentException("Unknown map event data");
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
                entryEventData.getDataKey(), entryEventData.getDataNewValue(), entryEventData.getDataOldValue(),
                mapService.getSerializationService());
    }

    private void dispatch0(IMapEvent event, EntryListener listener) {
        switch (event.getEventType()) {
            case ADDED:
                listener.entryAdded((EntryEvent) event);
                break;
            case EVICTED:
                listener.entryEvicted((EntryEvent) event);
                break;
            case UPDATED:
                listener.entryUpdated((EntryEvent) event);
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
