package com.hazelcast.map;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.NodeEngine;

import java.util.logging.Level;

/**
 * Contains map service event publishing service functionality.
 *
 * @see com.hazelcast.spi.EventPublishingService
 */
class MapEventPublishingService implements EventPublishingService<EventData, EntryListener> {

    private MapServiceContext mapServiceContext;
    private NodeEngine nodeEngine;
    private ILogger logger;

    protected MapEventPublishingService(MapServiceContext mapServiceContext, NodeEngine nodeEngine) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }


    private void incrementEventStats(IMapEvent event) {
        final String mapName = event.getName();
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            mapServiceContext.getLocalMapStatsProvider()
                    .getLocalMapStatsImpl(mapName).incrementReceivedEvents();
        }
    }

    @Override
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
        final Member member = nodeEngine.getClusterService().getMember(eventData.getCaller());
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
                nodeEngine.getSerializationService());
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
