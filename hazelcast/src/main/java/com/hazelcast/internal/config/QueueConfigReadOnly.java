/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.config;

import com.hazelcast.collection.IQueue;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Contains the configuration for an {@link IQueue}.
 */
public class QueueConfigReadOnly extends QueueConfig {

    public QueueConfigReadOnly(QueueConfig config) {
        super(config);
    }

    @Override
    public List<ItemListenerConfig> getItemListenerConfigs() {
        final List<ItemListenerConfig> itemListenerConfigs = super.getItemListenerConfigs();
        final List<ItemListenerConfig> readOnlyItemListenerConfigs
                = new ArrayList<ItemListenerConfig>(itemListenerConfigs.size());
        for (ItemListenerConfig itemListenerConfig : itemListenerConfigs) {
            readOnlyItemListenerConfigs.add(new ItemListenerConfigReadOnly(itemListenerConfig));
        }
        return Collections.unmodifiableList(readOnlyItemListenerConfigs);
    }

    @Override
    public QueueStoreConfig getQueueStoreConfig() {
        final QueueStoreConfig queueStoreConfig = super.getQueueStoreConfig();
        if (queueStoreConfig == null) {
            return null;
        }
        return new QueueStoreConfigReadOnly(queueStoreConfig);
    }

    @Override
    public QueueConfig setName(String name) {
        throw new UnsupportedOperationException("This config is read-only queue: " + getName());
    }

    @Override
    public QueueConfig setEmptyQueueTtl(int emptyQueueTtl) {
        throw new UnsupportedOperationException("This config is read-only queue: " + getName());
    }

    @Override
    public QueueConfig setMaxSize(int maxSize) {
        throw new UnsupportedOperationException("This config is read-only queue: " + getName());
    }

    @Override
    public QueueConfig setBackupCount(int backupCount) {
        throw new UnsupportedOperationException("This config is read-only queue: " + getName());
    }

    @Override
    public QueueConfig setAsyncBackupCount(int asyncBackupCount) {
        throw new UnsupportedOperationException("This config is read-only queue: " + getName());
    }

    @Override
    public QueueConfig setQueueStoreConfig(QueueStoreConfig queueStoreConfig) {
        throw new UnsupportedOperationException("This config is read-only queue: " + getName());
    }

    @Override
    public QueueConfig setStatisticsEnabled(boolean statisticsEnabled) {
        throw new UnsupportedOperationException("This config is read-only queue: " + getName());
    }

    @Override
    public QueueConfig addItemListenerConfig(ItemListenerConfig listenerConfig) {
        throw new UnsupportedOperationException("This config is read-only queue: " + getName());
    }

    @Override
    public QueueConfig setItemListenerConfigs(List<ItemListenerConfig> listenerConfigs) {
        throw new UnsupportedOperationException("This config is read-only queue: " + getName());
    }

    @Override
    public QueueConfig setSplitBrainProtectionName(String splitBrainProtectionName) {
        throw new UnsupportedOperationException("This config is read-only queue: " + getName());
    }

    @Override
    public QueueConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        throw new UnsupportedOperationException("This config is read-only queue: " + getName());
    }
}
