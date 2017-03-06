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

import com.hazelcast.core.ItemListener;

import java.util.EventListener;

/**
 * Contains the configuration for an Item Listener.
 */
public class ItemListenerConfig extends ListenerConfig {

    private boolean includeValue = true;

    private ItemListenerConfigReadOnly readOnly;

    public ItemListenerConfig() {
    }

    public ItemListenerConfig(String className, boolean includeValue) {
        super(className);
        this.includeValue = includeValue;
    }

    public ItemListenerConfig(ItemListener implementation, boolean includeValue) {
        super(implementation);
        this.includeValue = includeValue;
    }

    public ItemListenerConfig(ItemListenerConfig config) {
        includeValue = config.isIncludeValue();
        implementation = config.getImplementation();
        className = config.getClassName();
    }

    @Override
    public ItemListener getImplementation() {
        return (ItemListener) implementation;
    }

    public ItemListenerConfig setImplementation(final ItemListener implementation) {
        super.setImplementation(implementation);
        return this;
    }

    @Override
    public boolean isIncludeValue() {
        return includeValue;
    }

    public ItemListenerConfig setIncludeValue(boolean includeValue) {
        this.includeValue = includeValue;
        return this;
    }

    @Override
    public String toString() {
        return "ItemListenerConfig{includeValue=" + includeValue + '}';
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

        ItemListenerConfig that = (ItemListenerConfig) o;

        if (includeValue != that.includeValue) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (includeValue ? 1 : 0);
        return result;
    }

    @Override
    ItemListenerConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new ItemListenerConfigReadOnly(this);
        }
        return readOnly;
    }

    private static class ItemListenerConfigReadOnly extends ItemListenerConfig {

        ItemListenerConfigReadOnly(ItemListenerConfig config) {
            super(config);
        }

        @Override
        public ItemListenerConfig setImplementation(ItemListener implementation) {
            throw new UnsupportedOperationException("This config is read-only");
        }

        @Override
        public ItemListenerConfig setIncludeValue(boolean includeValue) {
            throw new UnsupportedOperationException("This config is read-only");
        }

        @Override
        public ListenerConfig setClassName(String className) {
            throw new UnsupportedOperationException("This config is read-only");
        }

        @Override
        public ListenerConfig setImplementation(EventListener implementation) {
            throw new UnsupportedOperationException("This config is read-only");
        }
    }
}
