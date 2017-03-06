/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.MapEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.map.listener.MapClearedListener;
import com.hazelcast.map.listener.MapEvictedListener;
import com.hazelcast.map.listener.MapListener;

import java.util.EventListener;

import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Configuration for EntryListener
 */
public class EntryListenerConfig extends ListenerConfig {

    private boolean local;

    private boolean includeValue = true;

    private EntryListenerConfigReadOnly readOnly;

    public EntryListenerConfig() {
    }

    public EntryListenerConfig(String className, boolean local, boolean includeValue) {
        super(className);
        this.local = local;
        this.includeValue = includeValue;
    }

    public EntryListenerConfig(EntryListener implementation, boolean local, boolean includeValue) {
        super(implementation);
        this.local = local;
        this.includeValue = includeValue;
    }

    public EntryListenerConfig(MapListener implementation, boolean local, boolean includeValue) {
        super(toEntryListener(implementation));
        this.local = local;
        this.includeValue = includeValue;
    }

    public EntryListenerConfig(EntryListenerConfig config) {
        includeValue = config.isIncludeValue();
        local = config.isLocal();
        implementation = config.getImplementation();
        className = config.getClassName();
    }

    @Override
    public ListenerConfig setImplementation(EventListener implementation) {
        isNotNull(implementation, "implementation");
        this.implementation = toEntryListener(implementation);
        this.className = null;
        return this;
    }

    @Override
    public EntryListener getImplementation() {
        return (EntryListener) implementation;
    }

    /**
     * This method provides a workaround by converting a MapListener to EntryListener
     * when it is added via EntryListenerConfig object.
     * <p/>
     * With this method, we are trying to fix two problems :
     * First, if we do not introduce the conversion in this method, {@link EntryListenerConfig#getImplementation}
     * will give {@link ClassCastException} with a MapListener implementation.
     * <p/>
     * Second goal of the conversion is to preserve backward compatibility.
     */
    private static EventListener toEntryListener(Object implementation) {
        if (implementation instanceof EntryListener) {
            return (EventListener) implementation;
        }

        if (implementation instanceof MapListener) {
            return new MapListenerToEntryListenerAdapter((MapListener) implementation);
        }

        throw new IllegalArgumentException(implementation + " is not an expected EventListener implementation."
                + " A valid one has to be an implementation of EntryListener or MapListener");

    }

    /**
     * Wraps a MapListener into an EntryListener.
     */
    public static class MapListenerToEntryListenerAdapter implements EntryListener, HazelcastInstanceAware {

        private final MapListener mapListener;

        public MapListenerToEntryListenerAdapter(MapListener mapListener) {
            this.mapListener = mapListener;
        }

        @Override
        public void entryAdded(EntryEvent event) {
            if (mapListener instanceof EntryAddedListener) {
                ((EntryAddedListener) mapListener).entryAdded(event);
            }
        }

        @Override
        public void entryEvicted(EntryEvent event) {
            if (mapListener instanceof EntryEvictedListener) {
                ((EntryEvictedListener) mapListener).entryEvicted(event);
            }
        }

        @Override
        public void entryRemoved(EntryEvent event) {
            if (mapListener instanceof EntryRemovedListener) {
                ((EntryRemovedListener) mapListener).entryRemoved(event);
            }
        }

        @Override
        public void entryUpdated(EntryEvent event) {
            if (mapListener instanceof EntryUpdatedListener) {
                ((EntryUpdatedListener) mapListener).entryUpdated(event);
            }
        }

        @Override
        public void mapCleared(MapEvent event) {
            if (mapListener instanceof MapClearedListener) {
                ((MapClearedListener) mapListener).mapCleared(event);
            }
        }

        @Override
        public void mapEvicted(MapEvent event) {
            if (mapListener instanceof MapEvictedListener) {
                ((MapEvictedListener) mapListener).mapEvicted(event);
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            if (mapListener instanceof HazelcastInstanceAware) {
                ((HazelcastInstanceAware) mapListener).setHazelcastInstance(hazelcastInstance);
            }
        }

        public MapListener getMapListener() {
            return mapListener;
        }
    }

    public EntryListenerConfig setImplementation(final EntryListener implementation) {
        super.setImplementation(implementation);
        return this;
    }

    @Override
    public boolean isLocal() {
        return local;
    }

    public EntryListenerConfig setLocal(boolean local) {
        this.local = local;
        return this;
    }

    @Override
    public boolean isIncludeValue() {
        return includeValue;
    }

    public EntryListenerConfig setIncludeValue(boolean includeValue) {
        this.includeValue = includeValue;
        return this;
    }

    @Override
    public String toString() {
        return "EntryListenerConfig{local=" + local + ", includeValue=" + includeValue + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        EntryListenerConfig that = (EntryListenerConfig) o;

        if (includeValue != that.includeValue) {
            return false;
        }
        if (local != that.local) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (local ? 1 : 0);
        result = 31 * result + (includeValue ? 1 : 0);
        return result;
    }

    @Override
    EntryListenerConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new EntryListenerConfigReadOnly(this);
        }
        return readOnly;
    }

    private static class EntryListenerConfigReadOnly extends EntryListenerConfig {

        EntryListenerConfigReadOnly(EntryListenerConfig config) {
            super(config);
        }

        @Override
        public EntryListenerConfig setImplementation(EntryListener implementation) {
            throw new UnsupportedOperationException("this config is read-only");
        }

        @Override
        public EntryListenerConfig setLocal(boolean local) {
            throw new UnsupportedOperationException("this config is read-only");
        }

        @Override
        public EntryListenerConfig setIncludeValue(boolean includeValue) {
            throw new UnsupportedOperationException("this config is read-only");
        }

        @Override
        public ListenerConfig setClassName(String className) {
            throw new UnsupportedOperationException("this config is read-only");
        }

        @Override
        public ListenerConfig setImplementation(EventListener implementation) {
            throw new UnsupportedOperationException("this config is read-only");
        }
    }
}
