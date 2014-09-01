package com.hazelcast.map.impl;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;

import java.util.List;
import java.util.concurrent.TimeUnit;


abstract class AbstractMapServiceContextSupport implements MapServiceContextSupport,
        MapServiceContextInterceptorSupport, MapServiceContextEventListenerSupport {

    protected NodeEngine nodeEngine;

    private MapServiceContext mapServiceContext;

    protected AbstractMapServiceContextSupport(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public void setMapServiceContext(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public long getNow() {
        return Clock.currentTimeMillis();
    }

    @Override
    public long convertTime(long seconds, TimeUnit unit) {
        return unit.toMillis(seconds);
    }

    @Override
    public long getExpirationTime(long ttl, long now) {
        if (ttl < 0 || now < 0) {
            throw new IllegalArgumentException("ttl and now parameters can not have negative values");
        }
        if (ttl == 0) {
            return Long.MAX_VALUE;
        }
        final long expirationTime = now + ttl;
        // detect potential overflow.
        if (expirationTime < 0L) {
            return Long.MAX_VALUE;
        }
        return expirationTime;
    }

    @Override
    public Object toObject(Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof Data) {
            return nodeEngine.toObject(data);
        } else {
            return data;
        }
    }

    @Override
    public Data toData(Object object, PartitioningStrategy partitionStrategy) {
        if (object == null) {
            return null;
        }
        if (object instanceof Data) {
            return (Data) object;
        } else {
            return nodeEngine.getSerializationService().toData(object, partitionStrategy);
        }
    }

    @Override
    public Data toData(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof Data) {
            return (Data) object;
        } else {
            return nodeEngine.getSerializationService().toData(object);
        }
    }

    @Override
    public boolean compare(String mapName, Object value1, Object value2) {
        if (value1 == null && value2 == null) {
            return true;
        }
        if (value1 == null) {
            return false;
        }
        if (value2 == null) {
            return false;
        }
        final MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        return mapContainer.getRecordFactory().isEquals(value1, value2);
    }

    @Override
    public void interceptAfterGet(String mapName, Object value) {
        List<MapInterceptor> interceptors = mapServiceContext.getMapContainer(mapName).getInterceptors();
        if (!interceptors.isEmpty()) {
            value = mapServiceContext.toObject(value);
            for (MapInterceptor interceptor : interceptors) {
                interceptor.afterGet(value);
            }
        }
    }

    @Override
    public Object interceptPut(String mapName, Object oldValue, Object newValue) {
        List<MapInterceptor> interceptors = mapServiceContext.getMapContainer(mapName).getInterceptors();
        Object result = null;
        if (!interceptors.isEmpty()) {
            result = mapServiceContext.toObject(newValue);
            oldValue = mapServiceContext.toObject(oldValue);
            for (MapInterceptor interceptor : interceptors) {
                Object temp = interceptor.interceptPut(oldValue, result);
                if (temp != null) {
                    result = temp;
                }
            }
        }
        return result == null ? newValue : result;
    }

    @Override
    public void interceptAfterPut(String mapName, Object newValue) {
        List<MapInterceptor> interceptors = mapServiceContext.getMapContainer(mapName).getInterceptors();
        if (!interceptors.isEmpty()) {
            newValue = mapServiceContext.toObject(newValue);
            for (MapInterceptor interceptor : interceptors) {
                interceptor.afterPut(newValue);
            }
        }
    }

    @Override
    public Object interceptRemove(String mapName, Object value) {
        List<MapInterceptor> interceptors = mapServiceContext.getMapContainer(mapName).getInterceptors();
        Object result = null;
        if (!interceptors.isEmpty()) {
            result = mapServiceContext.toObject(value);
            for (MapInterceptor interceptor : interceptors) {
                Object temp = interceptor.interceptRemove(result);
                if (temp != null) {
                    result = temp;
                }
            }
        }
        return result == null ? value : result;
    }

    @Override
    public void interceptAfterRemove(String mapName, Object value) {
        List<MapInterceptor> interceptors = mapServiceContext.getMapContainer(mapName).getInterceptors();
        if (!interceptors.isEmpty()) {
            for (MapInterceptor interceptor : interceptors) {
                value = mapServiceContext.toObject(value);
                interceptor.afterRemove(value);
            }
        }
    }

    @Override
    public String addInterceptor(String mapName, MapInterceptor interceptor) {
        return mapServiceContext.getMapContainer(mapName).addInterceptor(interceptor);
    }

    @Override
    public void removeInterceptor(String mapName, String id) {
        mapServiceContext.getMapContainer(mapName).removeInterceptor(id);
    }

    // todo interceptors should get a wrapped object which includes the serialized version
    @Override
    public Object interceptGet(String mapName, Object value) {
        List<MapInterceptor> interceptors = mapServiceContext.getMapContainer(mapName).getInterceptors();
        Object result = null;
        if (!interceptors.isEmpty()) {
            result = mapServiceContext.toObject(value);
            for (MapInterceptor interceptor : interceptors) {
                Object temp = interceptor.interceptGet(result);
                if (temp != null) {
                    result = temp;
                }
            }
        }
        return result == null ? value : result;
    }

    @Override
    public String addLocalEventListener(EntryListener entryListener, String mapName) {
        EventRegistration registration = nodeEngine.getEventService().
                registerLocalListener(mapServiceContext.serviceName(), mapName, entryListener);
        return registration.getId();
    }

    @Override
    public String addLocalEventListener(EntryListener entryListener, EventFilter eventFilter, String mapName) {
        EventRegistration registration = nodeEngine.getEventService().
                registerLocalListener(mapServiceContext.serviceName(), mapName, eventFilter, entryListener);
        return registration.getId();
    }

    @Override
    public String addEventListener(EntryListener entryListener, EventFilter eventFilter, String mapName) {
        EventRegistration registration = nodeEngine.getEventService().
                registerListener(mapServiceContext.serviceName(), mapName, eventFilter, entryListener);
        return registration.getId();
    }

    @Override
    public boolean removeEventListener(String mapName, String registrationId) {
        return nodeEngine.getEventService().deregisterListener(mapServiceContext.serviceName(), mapName, registrationId);
    }

    @Override
    public boolean hasRegisteredListener(String name) {
        final MapServiceContext mapServiceContext = this.mapServiceContext;
        final EventService eventService = mapServiceContext.getNodeEngine().getEventService();
        final String serviceName = mapServiceContext.serviceName();
        return eventService.hasEventRegistration(serviceName, name);
    }
}

