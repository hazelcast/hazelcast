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
 * Contains the configuration for an {@link com.hazelcast.core.MultiMap}.
 *
 * @deprecated this class will be removed in 4.0; it is meant for internal usage only.
 */
public class MultiMapConfigReadOnly extends MultiMapConfig {

    public MultiMapConfigReadOnly(MultiMapConfig defConfig) {
        super(defConfig);
    }

    @Override
    public List<EntryListenerConfig> getEntryListenerConfigs() {
        final List<EntryListenerConfig> listenerConfigs = super.getEntryListenerConfigs();
        final List<EntryListenerConfig> readOnlyListenerConfigs = new ArrayList<EntryListenerConfig>(listenerConfigs.size());
        for (EntryListenerConfig listenerConfig : listenerConfigs) {
            readOnlyListenerConfigs.add(listenerConfig.getAsReadOnly());
        }
        return Collections.unmodifiableList(readOnlyListenerConfigs);
    }

    @Override
    public MultiMapConfig setName(String name) {
        throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
    }

    @Override
    public MultiMapConfig setValueCollectionType(String valueCollectionType) {
        throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
    }

    @Override
    public MultiMapConfig setValueCollectionType(ValueCollectionType valueCollectionType) {
        throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
    }

    @Override
    public MultiMapConfig addEntryListenerConfig(EntryListenerConfig listenerConfig) {
        throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
    }

    @Override
    public MultiMapConfig setEntryListenerConfigs(List<EntryListenerConfig> listenerConfigs) {
        throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
    }

    @Override
    public MultiMapConfig setBinary(boolean binary) {
        throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
    }

    @Override
    public MultiMapConfig setSyncBackupCount(int syncBackupCount) {
        throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
    }

    @Override
    public MultiMapConfig setBackupCount(int backupCount) {
        throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
    }

    @Override
    public MultiMapConfig setAsyncBackupCount(int asyncBackupCount) {
        throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
    }

    @Override
    public MultiMapConfig setStatisticsEnabled(boolean statisticsEnabled) {
        throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
    }

    @Override
    public MultiMapConfig setQuorumName(String quorumName) {
        throw new UnsupportedOperationException("This config is read-only multimap: " + getName());
    }
}
