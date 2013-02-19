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

public final class QueueConfig implements DataSerializable {

    public final static int DEFAULT_MAX_SIZE = 0;
    public final static int DEFAULT_SYNC_BACKUP_COUNT = 1;
    public final static int DEFAULT_ASYNC_BACKUP_COUNT = 0;

    private String name;
    private List<ItemListenerConfig> listenerConfigs;
    private int syncBackupCount = DEFAULT_SYNC_BACKUP_COUNT;
    private int asyncBackupCount = DEFAULT_ASYNC_BACKUP_COUNT;
    private int maxSize = DEFAULT_MAX_SIZE;
    private QueueStoreConfig queueStoreConfig;

    public QueueConfig() {
    }

    public QueueConfig(QueueConfig config) {
        this();
        this.name = config.name;
        this.syncBackupCount = config.syncBackupCount;
        this.asyncBackupCount = config.asyncBackupCount;
        this.maxSize = config.maxSize;
        this.queueStoreConfig = config.queueStoreConfig;
        this.listenerConfigs = config.listenerConfigs;
    }

    public int getMaxSize() {
        return maxSize == 0 ? Integer.MAX_VALUE : maxSize;
    }

    public QueueConfig setMaxSize(int maxSize) {
        if (maxSize < 0){
            throw new IllegalArgumentException("Size of the queue can not be a negative value!");
        }
        this.maxSize = maxSize;
        return this;
    }

    public int getTotalBackupCount(){
        return syncBackupCount + asyncBackupCount;
    }

    public int getSyncBackupCount() {
        return syncBackupCount;
    }

    public QueueConfig setSyncBackupCount(int syncBackupCount) {
        this.syncBackupCount = syncBackupCount;
        return this;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public QueueConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = asyncBackupCount;
        return this;
    }

    public QueueStoreConfig getQueueStoreConfig() {
        return queueStoreConfig;
    }

    public QueueConfig setQueueStoreConfig(QueueStoreConfig queueStoreConfig) {
        this.queueStoreConfig = queueStoreConfig;
        return this;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     * @return this queue config
     */
    public QueueConfig setName(String name) {
        this.name = name;
        return this;
    }

    public QueueConfig addItemListenerConfig(ItemListenerConfig listenerConfig) {
        getItemListenerConfigs().add(listenerConfig);
        return this;
    }

    public List<ItemListenerConfig> getItemListenerConfigs() {
        if (listenerConfigs == null) {
            listenerConfigs = new ArrayList<ItemListenerConfig>();
        }
        return listenerConfigs;
    }

    public void setItemListenerConfigs(List<ItemListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
    }

    public boolean isCompatible(final QueueConfig queueConfig) {
        if (queueConfig == null) return false;
        return (name != null ? name.equals(queueConfig.name) : queueConfig.name == null);
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(syncBackupCount);
        out.writeInt(asyncBackupCount);
        out.writeInt(maxSize);
        //TODO store and listener configs
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        syncBackupCount = in.readInt();
        asyncBackupCount = in.readInt();
        maxSize = in.readInt();
    }
}
