/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;

import java.util.List;

import static com.hazelcast.map.impl.ListenerAdapters.createListenerAdapter;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

abstract class AbstractMapServiceContextSupport implements MapServiceContext {

    protected NodeEngine nodeEngine;

    protected AbstractMapServiceContextSupport(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public long getNow() {
        return Clock.currentTimeMillis();
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
        final MapContainer mapContainer = getMapContainer(mapName);
        return mapContainer.getRecordFactory().isEquals(value1, value2);
    }

    @Override
    public void interceptAfterGet(String mapName, Object value) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        if (!interceptors.isEmpty()) {
            value = toObject(value);
            for (MapInterceptor interceptor : interceptors) {
                interceptor.afterGet(value);
            }
        }
    }

    @Override
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

    @Override
    public void interceptAfterPut(String mapName, Object newValue) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        if (!interceptors.isEmpty()) {
            newValue = toObject(newValue);
            for (MapInterceptor interceptor : interceptors) {
                interceptor.afterPut(newValue);
            }
        }
    }

    @Override
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

    @Override
    public void interceptAfterRemove(String mapName, Object value) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        if (!interceptors.isEmpty()) {
            for (MapInterceptor interceptor : interceptors) {
                value = toObject(value);
                interceptor.afterRemove(value);
            }
        }
    }

    @Override
    public void addInterceptor(String id, String mapName, MapInterceptor interceptor) {
        MapContainer mapContainer = getMapContainer(mapName);
        mapContainer.addInterceptor(id, interceptor);
    }


    @Override
    public String generateInterceptorId(String mapName, MapInterceptor interceptor) {
        return interceptor.getClass().getName() + interceptor.hashCode();
    }

    @Override
    public void removeInterceptor(String mapName, String id) {
        getMapContainer(mapName).removeInterceptor(id);
    }

    // todo interceptors should get a wrapped object which includes the serialized version
    @Override
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

    @Override
    public String addLocalEventListener(Object listener, String mapName) {
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        EventRegistration registration = nodeEngine.getEventService().
                registerLocalListener(SERVICE_NAME, mapName, listenerAdaptor);
        return registration.getId();
    }

    @Override
    public String addLocalEventListener(Object listener, EventFilter eventFilter, String mapName) {
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        EventRegistration registration = nodeEngine.getEventService().
                registerLocalListener(SERVICE_NAME, mapName, eventFilter, listenerAdaptor);
        return registration.getId();
    }

    @Override
    public String addEventListener(Object listener, EventFilter eventFilter, String mapName) {
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        EventRegistration registration = nodeEngine.getEventService().
                registerListener(SERVICE_NAME, mapName, eventFilter, listenerAdaptor);
        return registration.getId();
    }

    @Override
    public String addPartitionLostListener(MapPartitionLostListener listener, String mapName) {
        final ListenerAdapter listenerAdapter = new InternalMapPartitionLostListenerAdapter(listener);
        final EventFilter filter = new MapPartitionLostEventFilter();
        final EventRegistration registration = nodeEngine.getEventService().registerListener(SERVICE_NAME, mapName, filter,
                listenerAdapter);
        return registration.getId();
    }

    @Override
    public boolean removeEventListener(String mapName, String registrationId) {
        return nodeEngine.getEventService().deregisterListener(SERVICE_NAME, mapName, registrationId);
    }

    @Override
    public boolean removePartitionLostListener(String mapName, String registrationId) {
        return nodeEngine.getEventService().deregisterListener(SERVICE_NAME, mapName, registrationId);
    }

}

