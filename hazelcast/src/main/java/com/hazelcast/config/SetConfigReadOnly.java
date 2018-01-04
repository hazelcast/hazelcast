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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Contains configuration for Set(read only)
 *
 * @deprecated this class will be removed in 4.0; it is meant for internal usage only.
 */
public class SetConfigReadOnly extends SetConfig {

    public SetConfigReadOnly(SetConfig config) {
        super(config);
    }

    @Override
    public List<ItemListenerConfig> getItemListenerConfigs() {
        final List<ItemListenerConfig> itemListenerConfigs = super.getItemListenerConfigs();
        final List<ItemListenerConfig> readOnlyItemListenerConfigs
                = new ArrayList<ItemListenerConfig>(itemListenerConfigs.size());
        for (ItemListenerConfig itemListenerConfig : itemListenerConfigs) {
            readOnlyItemListenerConfigs.add(itemListenerConfig.getAsReadOnly());
        }
        return Collections.unmodifiableList(readOnlyItemListenerConfigs);
    }

    @Override
    public SetConfig setName(String name) {
        throw new UnsupportedOperationException("This config is read-only set: " + getName());
    }

    @Override
    public SetConfig setItemListenerConfigs(List<ItemListenerConfig> listenerConfigs) {
        throw new UnsupportedOperationException("This config is read-only set: " + getName());
    }

    @Override
    public SetConfig setBackupCount(int backupCount) {
        throw new UnsupportedOperationException("This config is read-only set: " + getName());
    }

    @Override
    public SetConfig setAsyncBackupCount(int asyncBackupCount) {
        throw new UnsupportedOperationException("This config is read-only set: " + getName());
    }

    @Override
    public SetConfig setMaxSize(int maxSize) {
        throw new UnsupportedOperationException("This config is read-only set: " + getName());
    }

    @Override
    public SetConfig setStatisticsEnabled(boolean statisticsEnabled) {
        throw new UnsupportedOperationException("This config is read-only set: " + getName());
    }

    @Override
    public void addItemListenerConfig(ItemListenerConfig itemListenerConfig) {
        throw new UnsupportedOperationException("This config is read-only set: " + getName());
    }

    @Override
    public SetConfig setQuorumName(String quorumName) {
        throw new UnsupportedOperationException("This config is read-only set: " + getName());
    }
}
