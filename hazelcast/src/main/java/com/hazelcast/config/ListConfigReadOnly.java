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

package com.hazelcast.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Contains the configuration for an {@link com.hazelcast.core.IList} (read-only).
 */
public class ListConfigReadOnly extends ListConfig {

    public ListConfigReadOnly(ListConfig config) {
        super(config);
    }

    public List<ItemListenerConfig> getItemListenerConfigs() {
        final List<ItemListenerConfig> itemListenerConfigs = super.getItemListenerConfigs();
        final List<ItemListenerConfig> readOnlyItemListenerConfigs =
                new ArrayList<ItemListenerConfig>(itemListenerConfigs.size());
        for (ItemListenerConfig itemListenerConfig : itemListenerConfigs) {
            readOnlyItemListenerConfigs.add(itemListenerConfig.getAsReadOnly());
        }
        return Collections.unmodifiableList(readOnlyItemListenerConfigs);
    }

    public ListConfig setName(String name) {
        throw new UnsupportedOperationException("This config is read-only list: " + getName());
    }

    public ListConfig setItemListenerConfigs(List<ItemListenerConfig> listenerConfigs) {
        throw new UnsupportedOperationException("This config is read-only list: " + getName());
    }

    public ListConfig setBackupCount(int backupCount) {
        throw new UnsupportedOperationException("This config is read-only list: " + getName());
    }

    public ListConfig setAsyncBackupCount(int asyncBackupCount) {
        throw new UnsupportedOperationException("This config is read-only list: " + getName());
    }

    public ListConfig setMaxSize(int maxSize) {
        throw new UnsupportedOperationException("This config is read-only list: " + getName());
    }

    public ListConfig setStatisticsEnabled(boolean statisticsEnabled) {
        throw new UnsupportedOperationException("This config is read-only list: " + getName());
    }

    public void addItemListenerConfig(ItemListenerConfig itemListenerConfig) {
        throw new UnsupportedOperationException("This config is read-only list: " + getName());
    }
}
