package com.hazelcast.client.impl.querycache.subscriber;

import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientListenerServiceImpl;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.impl.EventData;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.client.MapAddListenerAdapterRequest;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.event.BatchEventData;
import com.hazelcast.map.impl.querycache.event.BatchIMapEvent;
import com.hazelcast.map.impl.querycache.event.LocalEntryEventData;
import com.hazelcast.map.impl.querycache.event.SingleEventData;
import com.hazelcast.map.impl.querycache.event.SingleIMapEvent;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.impl.eventservice.impl.EmptyFilter;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;
import com.hazelcast.util.executor.TimeoutRunnable;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.querycache.ListenerRegistrationHelper.generateListenerName;
import static com.hazelcast.map.impl.querycache.subscriber.EventPublisherHelper.createIMapEvent;
import static com.hazelcast.map.impl.querycache.subscriber.QueryCacheEventListenerAdapters.createQueryCacheListenerAdaptor;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Client side event service implementation for query cache.
 *
 * @see QueryCacheEventService
 */
public class ClientQueryCacheEventService implements QueryCacheEventService {

    private static final int EVENT_QUEUE_TIMEOUT_MILLIS = 500;

    private static final ConstructorFunction<String, QueryCacheToListenerMapper> REGISTRY_CONSTRUCTOR =
            new ConstructorFunction<String, QueryCacheToListenerMapper>() {
                @Override
                public QueryCacheToListenerMapper createNew(String arg) {
                    return new QueryCacheToListenerMapper();
                }
            };

    private final ILogger logger = Logger.getLogger(getClass());
    private final StripedExecutor executor;
    private final ConcurrentMap<String, QueryCacheToListenerMapper> registrations;
    private final SerializationService serializationService;
    private final ClientListenerService listenerService;

    public ClientQueryCacheEventService(ClientContext clientContext) {
        ClientListenerServiceImpl listenerService = (ClientListenerServiceImpl) clientContext.getListenerService();
        this.listenerService = listenerService;
        this.serializationService = clientContext.getSerializationService();
        this.executor = listenerService.getEventExecutor();
        this.registrations = new ConcurrentHashMap<String, QueryCacheToListenerMapper>();
    }

    @Override
    public boolean hasListener(String mapName, String cacheName) {
        QueryCacheToListenerMapper queryCacheToListenerMapper = registrations.get(mapName);
        if (queryCacheToListenerMapper == null) {
            return false;
        }

        Collection<ListenerInfo> infos = queryCacheToListenerMapper.getListenerInfos(cacheName);
        if (infos.isEmpty()) {
            return false;
        }
        return true;
    }

    @Override
    public void sendEventToSubscriber(String name, Object eventData, int orderKey) {
        // this is already subscriber side. So no need to implement it for subscriber side.
        throw new UnsupportedOperationException();
    }

    @Override
    public void publish(String mapName, String cacheName, Object event, int orderKey) {
        checkHasText(mapName, "mapName");
        checkHasText(cacheName, "cacheName");
        checkNotNull(event, "event cannot be null");

        Collection<ListenerInfo> listeners = getListeners(mapName, cacheName);
        for (ListenerInfo info : listeners) {
            try {
                executor.execute(new EventDispatcher(event, info, orderKey, serializationService, EVENT_QUEUE_TIMEOUT_MILLIS));
            } catch (RejectedExecutionException e) {
                // TODO Should we notify user when we overloaded?
                logger.warning("EventQueue overloaded! Can not process IMap=[" + mapName + "]"
                        + ", QueryCache=[ " + cacheName + "]" + ", Event=[" + event + "]");
            }
        }
    }

    @Override
    public String listenPublisher(String mapName, String cacheName, ListenerAdapter adapter) {
        String listenerName = generateListenerName(mapName, cacheName);
        MapAddListenerAdapterRequest request = new MapAddListenerAdapterRequest(listenerName);
        EventHandler<EventData> handler = createHandler(adapter);
        return listenerService.startListening(request, null, handler);
    }

    @Override
    public String addListener(String mapName, String cacheName, MapListener listener) {
        return addListener(mapName, cacheName, listener, null);
    }

    @Override
    public String addListener(String mapName, String cacheName, MapListener listener, EventFilter filter) {
        checkHasText(mapName, "mapName");
        checkHasText(cacheName, "cacheName");
        checkNotNull(listener, "listener cannot be null");

        QueryCacheToListenerMapper queryCacheToListenerMapper = getOrPutIfAbsent(registrations, mapName, REGISTRY_CONSTRUCTOR);
        ListenerAdapter listenerAdaptor = createQueryCacheListenerAdaptor(listener);
        return queryCacheToListenerMapper.addListener(cacheName, listenerAdaptor, filter);
    }

    @Override
    public boolean removeListener(String mapName, String cacheName, String id) {
        checkHasText(mapName, "mapName");
        checkHasText(cacheName, "cacheName");
        checkHasText(id, "id");

        QueryCacheToListenerMapper queryCacheToListenerMapper = getOrPutIfAbsent(registrations, mapName, REGISTRY_CONSTRUCTOR);
        return queryCacheToListenerMapper.removeListener(cacheName, id);
    }

    private EventHandler<EventData> createHandler(final ListenerAdapter adapter) {
        return new EventHandler<EventData>() {

            @Override
            public void handle(EventData eventData) {

                IMapEvent iMapEvent = null;
                if (eventData instanceof SingleEventData) {
                    iMapEvent = new SingleIMapEvent((SingleEventData) eventData);
                } else if (eventData instanceof BatchEventData) {
                    iMapEvent = new BatchIMapEvent((BatchEventData) eventData);
                }

                adapter.onEvent(iMapEvent);
            }

            @Override
            public void beforeListenerRegister() {
                // NOP
            }

            @Override
            public void onListenerRegister() {
                // NOP
            }
        };
    }

    private Collection<ListenerInfo> getListeners(String mapName, String cacheName) {
        QueryCacheToListenerMapper queryCacheToListenerMapper = registrations.get(mapName);
        if (queryCacheToListenerMapper == null) {
            return Collections.emptySet();
        }
        return queryCacheToListenerMapper.getListenerInfos(cacheName);
    }

    /**
     * Dispatches an event to a listener.
     */
    private static class EventDispatcher implements StripedRunnable, TimeoutRunnable {

        private final Object event;
        private final ListenerInfo listenerInfo;
        private final int orderKey;
        private final long timeoutMs;
        private final SerializationService serializationService;

        public EventDispatcher(Object event, ListenerInfo listenerInfo, int orderKey,
                               SerializationService serializationService, long timeoutMs) {
            this.event = event;
            this.listenerInfo = listenerInfo;
            this.orderKey = orderKey;
            this.timeoutMs = timeoutMs;
            this.serializationService = serializationService;
        }

        @Override
        public int getKey() {
            return orderKey;
        }

        @Override
        public void run() {
            EventData eventData = (EventData) event;
            EventFilter filter = listenerInfo.getFilter();

            if (eventData instanceof LocalEntryEventData
                    && eventData.getEventType() != EventLostEvent.EVENT_TYPE) {

                LocalEntryEventData localEntryEventData = (LocalEntryEventData) eventData;
                if (!canPassFilter(localEntryEventData, filter)) {
                    return;
                }
            }

            IMapEvent event = createIMapEvent(eventData, filter, null, serializationService);
            ListenerAdapter listenerAdapter = listenerInfo.getListenerAdapter();
            listenerAdapter.onEvent(event);
        }


        private boolean canPassFilter(LocalEntryEventData eventData, EventFilter filter) {
            if (filter == null || filter instanceof EmptyFilter) {
                return true;
            }
            Object value = getValueOrOldValue(eventData);
            Object key = eventData.getKey();
            Data keyData = eventData.getKeyData();
            QueryEntry entry = new QueryEntry(serializationService, keyData, key, value);
            return filter.eval(entry);
        }


        private Object getValueOrOldValue(LocalEntryEventData localEntryEventData) {
            Object value = localEntryEventData.getValue();
            return value != null ? value : localEntryEventData.getOldValue();
        }

        @Override
        public long getTimeout() {
            return timeoutMs;
        }

        @Override
        public TimeUnit getTimeUnit() {
            return TimeUnit.MILLISECONDS;
        }

    }
}

