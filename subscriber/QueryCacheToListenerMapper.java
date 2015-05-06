package com.hazelcast.client.impl.querycache.subscriber;

import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.CollectionUtil.isEmpty;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * This class includes mappings of `cacheName --to-> listener collections`.
 */
public class QueryCacheToListenerMapper {

    private static final ConstructorFunction<String, Collection<ListenerInfo>> LISTENER_SET_CONSTRUCTOR
            = new ConstructorFunction<String, Collection<ListenerInfo>>() {
        @Override
        public Collection<ListenerInfo> createNew(String arg) {
            return Collections.newSetFromMap(new ConcurrentHashMap<ListenerInfo, Boolean>());
        }
    };

    private final ConcurrentMap<String, Collection<ListenerInfo>> registrations;

    public QueryCacheToListenerMapper() {
        this.registrations = new ConcurrentHashMap<String, Collection<ListenerInfo>>();
    }

    public String addListener(String cacheName, ListenerAdapter listenerAdapter, EventFilter filter) {
        Collection<ListenerInfo> adapters = getOrPutIfAbsent(registrations, cacheName, LISTENER_SET_CONSTRUCTOR);
        String id = UUID.randomUUID().toString();
        ListenerInfo info = new ListenerInfo(filter, listenerAdapter, id);
        adapters.add(info);
        return id;
    }

    public boolean removeListener(String cacheName, String id) {
        Collection<ListenerInfo> adapters = getOrPutIfAbsent(registrations, cacheName, LISTENER_SET_CONSTRUCTOR);
        Iterator<ListenerInfo> iterator = adapters.iterator();
        while (iterator.hasNext()) {
            ListenerInfo listenerInfo = iterator.next();
            String listenerInfoId = listenerInfo.getId();
            if (listenerInfoId.equals(id)) {
                iterator.remove();
                return true;
            }
        }
        return false;
    }

    public Collection<ListenerInfo> getListenerInfos(String cacheName) {
        Collection<ListenerInfo> infos = registrations.get(cacheName);
        return isEmpty(infos) ? Collections.EMPTY_SET : infos;
    }
}
