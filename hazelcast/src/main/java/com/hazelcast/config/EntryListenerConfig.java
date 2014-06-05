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

package com.hazelcast.config;

import com.hazelcast.core.EntryListener;

public class EntryListenerConfig extends ListenerConfig {

    private boolean local = false;

    private boolean includeValue = true;

    private EntryListenerConfigReadOnly readOnly;

    public EntryListenerConfig() {
        super();
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

    public EntryListenerConfig(EntryListenerConfig config) {
        includeValue = config.isIncludeValue();
        local = config.isLocal();
        implementation = config.getImplementation();
        className = config.getClassName();
    }

    public EntryListenerConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new EntryListenerConfigReadOnly(this);
        }
        return readOnly;
    }

    public EntryListener getImplementation() {
        return (EntryListener) implementation;
    }

    public EntryListenerConfig setImplementation(final EntryListener implementation) {
        super.setImplementation(implementation);
        return this;
    }

    public boolean isLocal() {
        return local;
    }

    public EntryListenerConfig setLocal(boolean local) {
        this.local = local;
        return this;
    }

    public boolean isIncludeValue() {
        return includeValue;
    }

    public EntryListenerConfig setIncludeValue(boolean includeValue) {
        this.includeValue = includeValue;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("EntryListenerConfig");
        sb.append("{local=").append(local);
        sb.append(", includeValue=").append(includeValue);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        EntryListenerConfig that = (EntryListenerConfig) o;

        if (includeValue != that.includeValue) return false;
        if (local != that.local) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (local ? 1 : 0);
        result = 31 * result + (includeValue ? 1 : 0);
        return result;
    }
}
