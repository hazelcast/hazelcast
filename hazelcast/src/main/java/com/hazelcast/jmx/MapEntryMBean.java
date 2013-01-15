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

package com.hazelcast.jmx;

import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEntry;

import javax.management.ObjectName;

/**
 * MBean for MapEntry
 *
 * @author Marco Ferrante, DISI - University of Genova
 */
@JMXDescription("A Entry in a Map")
public class MapEntryMBean extends AbstractMBean<MapEntry> {

    public static ObjectName buildObjectName(ObjectName mapName, Object key) throws Exception {
        StringBuffer sb = new StringBuffer();
        sb.append(mapName.getDomain()).append(':');
        sb.append(mapName.getKeyPropertyListString());
        sb.append(',');
        if (key instanceof String) {
            sb.append("key=").append(ObjectName.quote(key.toString()));
        } else {
            sb.append("name=").append(key.getClass().getName()).append('@').append(Integer.toHexString(key.hashCode()));
        }
        return new ObjectName(sb.toString());
    }

    public static ObjectName buildObjectNameFilter(ObjectName mapName) throws Exception {
        StringBuffer sb = new StringBuffer();
        sb.append(mapName.getDomain()).append(':');
        sb.append(mapName.getKeyPropertyListString());
        sb.append(',');
        sb.append("*");
        return new ObjectName(sb.toString());
    }

    @SuppressWarnings({"unused", "unchecked"})
    private final IMap map;
    private final Object key;

    @SuppressWarnings("unchecked")
    public MapEntryMBean(IMap map, Object key) {
        super(map.getMapEntry(key), null);  // Is immutable...
        this.map = map;
        this.key = key;
    }

    protected MapEntry refresh() {
        return map.getMapEntry(key);
    }

    @JMXAttribute("KeyClass")
    @JMXDescription("Class of the key")
    public Class<?> getKeyClass() {
        return key.getClass();
    }
    // Interferes with lastAccessTime?

    @JMXAttribute("ValueClass")
    @JMXDescription("Class of the value")
    public Class<?> getValueClass() {
        try {
            if (!map.containsKey(key)) {
                // Unregister removed entries
                if (mbeanServer.isRegistered(getObjectName())) {
                    mbeanServer.unregisterMBean(getObjectName());
                    return null;
                }
            }
            return map.get(key).getClass();
        } catch (Exception cnfe) {
            return String.class;
        } catch (Throwable t) {
            return null;
        }
    }

    @JMXAttribute("Cost")
    @JMXDescription("Cost")
    public long getCost() {
        return getManagedObject().getCost();
    }

    @JMXAttribute("CreationTime")
    @JMXDescription("Creation time")
    long getCreationTime() {
        return getManagedObject().getCreationTime();
    }

    @JMXAttribute("ExpirationTime")
    @JMXDescription("Expiration time")
    long getExpirationTime() {
        return getManagedObject().getExpirationTime();
    }

    @JMXAttribute("Hits")
    @JMXDescription("Hits")
    int getHits() {
        return getManagedObject().getHits();
    }

    @JMXAttribute("LastAccessTime")
    @JMXDescription("Last access time")
    long getLastAccessTime() {
        return getManagedObject().getLastAccessTime();
    }

    @JMXAttribute("LastUpdateTime")
    @JMXDescription("Last update time")
    long getLastUpdateTime() {
        return getManagedObject().getLastUpdateTime();
    }

    @JMXAttribute("Version")
    @JMXDescription("Version")
    long getVersion() {
        return getManagedObject().getVersion();
    }

    @JMXAttribute("Valid")
    @JMXDescription("Is valid")
    boolean isValid() {
        return getManagedObject().isValid();
    }
}
