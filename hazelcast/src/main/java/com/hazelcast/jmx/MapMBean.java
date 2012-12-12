/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.logging.Level;

/**
 * MBean for Map
 *
 * @author Marco Ferrante, DISI - University of Genova
 */
@SuppressWarnings("unchecked")
@JMXDescription("A distributed Map")
public class MapMBean extends AbstractMBean<IMap> {

    protected EntryListener listener;

    public MapMBean(IMap managedObject, ManagementService managementService) {
        super(managedObject, managementService);
    }

    @Override
    public ObjectNameSpec getNameSpec() {
        return getParentName().getNested("Map", getName());
    }

    @Override
    public void postRegister(Boolean registrationDone) {
        super.postRegister(registrationDone);
        if (!registrationDone) {
            return;
        }
        if (managementService.showDetails()) {
            listener = new EntryListener() {

                public void entryAdded(EntryEvent event) {
                    addEntry(event.getKey());
                }

                public void entryRemoved(EntryEvent event) {
                    removeEntry(event.getKey());
                }

                public void entryUpdated(EntryEvent event) {
                    // Nothing to do
                }

                public void entryEvicted(EntryEvent event) {
                    entryRemoved(event);
                }
            };
            getManagedObject().addEntryListener(listener, false);
            // Add existing entries
            for (Object key : getManagedObject().keySet()) {
                addEntry(key);
            }
        }
    }

    @Override
    public void preDeregister() throws Exception {
        if (listener != null) {
            getManagedObject().removeEntryListener(listener);
            listener = null;
            // Remove all entries
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            Set<ObjectName> entries = mbs.queryNames(MapEntryMBean.buildObjectNameFilter(getObjectName()), null);
            for (ObjectName name : entries) {
                if (getObjectName().equals(name)) {
                    // Do not deregister itself
                    continue;
                }
                mbs.unregisterMBean(name);
            }
        }
    }

    @JMXOperation("clear")
    @JMXDescription("Clear map")
    public void clear() {
        getManagedObject().clear();
    }

    @JMXOperation("values")
    @JMXDescription("Values")
    public String values(final String query) {
        final Predicate predicate = query != null && query.trim().length() > 0 ?
                new SqlPredicate(query) : null;
        final Collection values = predicate != null ?
                getManagedObject().values(predicate) :
                getManagedObject().values();
        final List list = new ArrayList(values);
        return list.toString();
    }

    @JMXOperation("entrySet")
    @JMXDescription("EntrySet")
    public String entrySet(final String query) {
        final Predicate predicate = query != null && query.trim().length() > 0 ?
                new SqlPredicate(query) : null;
        final Collection<Map.Entry> values = predicate != null ?
                getManagedObject().entrySet(predicate) :
                getManagedObject().entrySet();
        final StringBuilder sb = new StringBuilder().append("{");
        for (final Iterator<Map.Entry> it = values.iterator(); it.hasNext(); ) {
            final Map.Entry next = it.next();
            // workaround due to bug with hasNext
            if (sb.length() > 1) {
                sb.append(", ");
            }
            sb.append("key:").append(next.getKey()).append(", value:").append(next.getValue());
        }
        return sb.append("}").toString();
    }

    @JMXAttribute("Config")
    @JMXDescription("Map configuration")
    public String getConfig() {
        final MapConfig config = managementService.getInstance().getConfig().getMapConfig(getName());
        return config.toString();
    }

    protected void addEntry(Object key) {
        try {
            ObjectName entryName = MapEntryMBean.buildObjectName(getObjectName(), key);
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            if (!mbs.isRegistered(entryName)) {
                MapEntryMBean mbean = new MapEntryMBean(getManagedObject(), key);
                mbs.registerMBean(mbean, entryName);
            }
        } catch (Exception e) {
            logger.log(Level.FINE, "Unable to register MapEntry MBeans", e);
        }
    }

    protected void removeEntry(Object key) {
        try {
            ObjectName entryName = MapEntryMBean.buildObjectName(getObjectName(), key);
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            if (mbs.isRegistered(entryName)) {
                mbs.unregisterMBean(entryName);
            }
        } catch (Exception e) {
            logger.log(Level.FINE, "Unable to unregister MapEntry MBeans", e);
        }
    }

    @JMXAttribute("Name")
    @JMXDescription("Registration name of the map")
    public String getName() {
        return getManagedObject().getName();
    }

    @JMXAttribute("Size")
    @JMXDescription("Current size")
    public int getSize() {
        return getManagedObject().size();
    }
}
