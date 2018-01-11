/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Contains the configuration for an Item Listener.
 */
public class ItemListenerConfig extends ListenerConfig {

    private boolean includeValue = true;

    private transient ItemListenerConfigReadOnly readOnly;

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

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    @Override
    public ItemListenerConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new ItemListenerConfigReadOnly(this);
        }
        return readOnly;
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
    public int getId() {
        return ConfigDataSerializerHook.ITEM_LISTENER_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeBoolean(includeValue);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        includeValue = in.readBoolean();
    }
}
