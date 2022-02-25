/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.EventListener;

import static com.hazelcast.internal.util.Preconditions.checkInstanceOf;
import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * Configuration for {@link com.hazelcast.core.EntryListener}
 */
public class EntryListenerConfig extends ListenerConfig {

    private boolean local;
    private boolean includeValue = true;

    public EntryListenerConfig() {
    }

    public EntryListenerConfig(String className, boolean local, boolean includeValue) {
        super(className);
        this.local = local;
        this.includeValue = includeValue;
    }

    public EntryListenerConfig(MapListener implementation, boolean local, boolean includeValue) {
        super(implementation);
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
        checkInstanceOf(MapListener.class, isNotNull(implementation, "implementation"));
        super.setImplementation(implementation);
        return this;
    }

    public EntryListenerConfig setImplementation(MapListener implementation) {
        super.setImplementation(implementation);
        return this;
    }

    @Override
    public MapListener getImplementation() {
        return (MapListener) implementation;
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
    public int getClassId() {
        return ConfigDataSerializerHook.ENTRY_LISTENER_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeBoolean(local);
        out.writeBoolean(includeValue);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        local = in.readBoolean();
        includeValue = in.readBoolean();
    }
}
