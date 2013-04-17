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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiMapConfig implements DataSerializable {


    public final static int DEFAULT_SYNC_BACKUP_COUNT = 1;
    public final static int DEFAULT_ASYNC_BACKUP_COUNT = 0;

    private String name;
    private String valueCollectionType = ValueCollectionType.SET.toString();
    private List<EntryListenerConfig> listenerConfigs;
    private boolean binary = true;
    private int syncBackupCount = DEFAULT_SYNC_BACKUP_COUNT;
    private int asyncBackupCount = DEFAULT_ASYNC_BACKUP_COUNT;
    private boolean statisticsEnabled = true;

    public MultiMapConfig() {
    }

    public MultiMapConfig(MultiMapConfig defConfig) {
        this.name = defConfig.getName();
        this.valueCollectionType = defConfig.getValueCollectionType().toString();
        this.binary = defConfig.binary;
        this.syncBackupCount = defConfig.syncBackupCount;
        this.asyncBackupCount = defConfig.asyncBackupCount;
    }

    public enum ValueCollectionType {
        SET, LIST
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(valueCollectionType);
        out.writeBoolean(binary);
        out.writeInt(syncBackupCount);
        out.writeInt(asyncBackupCount);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        valueCollectionType = in.readUTF();
        binary = in.readBoolean();
        syncBackupCount = in.readInt();
        asyncBackupCount = in.readInt();
    }

    public String getName() {
        return name;
    }

    public MultiMapConfig setName(String name) {
        this.name = name;
        return this;
    }

    public ValueCollectionType getValueCollectionType() {
        return ValueCollectionType.valueOf(valueCollectionType.toUpperCase());
    }

    public MultiMapConfig setValueCollectionType(String valueCollectionType) {
        this.valueCollectionType = valueCollectionType;
        return this;
    }

    public MultiMapConfig setValueCollectionType(ValueCollectionType valueCollectionType) {
        this.valueCollectionType = valueCollectionType.toString();
        return this;
    }

    public MultiMapConfig addEntryListenerConfig(EntryListenerConfig listenerConfig) {
        getEntryListenerConfigs().add(listenerConfig);
        return this;
    }

    public List<EntryListenerConfig> getEntryListenerConfigs() {
        if (listenerConfigs == null) {
            listenerConfigs = new ArrayList<EntryListenerConfig>();
        }
        return listenerConfigs;
    }

    public MultiMapConfig setEntryListenerConfigs(List<EntryListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
        return this;
    }

    public boolean isBinary() {
        return binary;
    }

    public MultiMapConfig setBinary(boolean binary) {
        this.binary = binary;
        return this;
    }

    public int getSyncBackupCount() {
        return syncBackupCount;
    }

    public MultiMapConfig setSyncBackupCount(int syncBackupCount) {
        this.syncBackupCount = syncBackupCount;
        return this;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public MultiMapConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = asyncBackupCount;
        return this;
    }

    public int getTotalBackupCount() {
        return syncBackupCount + asyncBackupCount;
    }

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public void setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MultiMapConfig");
        sb.append("{name='").append(name).append('\'');
        sb.append(", valueCollectionType='").append(valueCollectionType).append('\'');
        sb.append(", listenerConfigs=").append(listenerConfigs);
        sb.append(", binary=").append(binary);
        sb.append(", syncBackupCount=").append(syncBackupCount);
        sb.append(", asyncBackupCount=").append(asyncBackupCount);
        sb.append('}');
        return sb.toString();
    }
}
