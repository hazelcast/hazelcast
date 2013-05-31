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

import com.hazelcast.core.ItemListener;
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
    private int backupCount = DEFAULT_SYNC_BACKUP_COUNT;
    private int asyncBackupCount = DEFAULT_ASYNC_BACKUP_COUNT;
    private int maxSize = DEFAULT_MAX_SIZE;
    private QueueStoreConfig queueStoreConfig;
    private boolean statisticsEnabled = true;

    public QueueConfig() {
    }

    public QueueConfig(QueueConfig config) {
        this();
        this.name = config.name;
        this.backupCount = config.backupCount;
        this.asyncBackupCount = config.asyncBackupCount;
        this.maxSize = config.maxSize;
        this.queueStoreConfig = config.queueStoreConfig;
        this.listenerConfigs = config.listenerConfigs;
    }

    public int getMaxSize() {
        return maxSize == 0 ? Integer.MAX_VALUE : maxSize;
    }

    public QueueConfig setMaxSize(int maxSize) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("Size of the queue can not be a negative value!");
        }
        this.maxSize = maxSize;
        return this;
    }

    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    public int getBackupCount() {
        return backupCount;
    }

    public QueueConfig setBackupCount(int backupCount) {
        this.backupCount = backupCount;
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

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public void setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
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
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);
        out.writeInt(maxSize);
        if (queueStoreConfig != null){
            out.writeBoolean(true);
            queueStoreConfig.writeData(out);
        } else {
            out.writeBoolean(false);
        }

        out.writeInt(listenerConfigs == null ? -1 : listenerConfigs.size());
        if (listenerConfigs != null){
            for (ItemListenerConfig listenerConfig : listenerConfigs) {
                out.writeUTF(listenerConfig.getClassName());
                out.writeBoolean(listenerConfig.isIncludeValue());
                out.writeObject(listenerConfig.getImplementation());
            }
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();
        maxSize = in.readInt();
        if (in.readBoolean()){
            queueStoreConfig = new QueueStoreConfig();
            queueStoreConfig.readData(in);
        }
        int size = in.readInt();
        if (size != -1){
            listenerConfigs = new ArrayList<ItemListenerConfig>(size);
            for (int i=0; i<size; i++){
                final String className = in.readUTF();
                final boolean include = in.readBoolean();
                final ItemListener implementation = in.readObject();
                final ItemListenerConfig itemListenerConfig = new ItemListenerConfig(className, include);
                itemListenerConfig.setImplementation(implementation);
                listenerConfigs.add(itemListenerConfig);
            }
        }
    }
}
