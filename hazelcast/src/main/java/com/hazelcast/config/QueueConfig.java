/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class QueueConfig implements DataSerializable {

    public final static int DEFAULT_MAX_SIZE_PER_JVM = 1000;

    private String name;
    private String backingMapRef;
    private int maxSizePerJVM = DEFAULT_MAX_SIZE_PER_JVM;
    private List<ItemListenerConfig> listenerConfigs;
    private int syncBackupCount;
    private int asyncBackupCount;

    public QueueConfig() {
    }

    public QueueConfig(QueueConfig config) {
        this.name = config.name;
        this.backingMapRef = config.backingMapRef;
//        this.maxSizePerJVM = config.maxSizePerJVM; //TODO change name and value from config
        this.maxSizePerJVM = 3;
        this.syncBackupCount = 1;
        this.asyncBackupCount = 1;
    }

    public int getSyncBackupCount() {
        return syncBackupCount;
    }

    public void setSyncBackupCount(int syncBackupCount) {
        this.syncBackupCount = syncBackupCount;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public void setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = asyncBackupCount;
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
        if (backingMapRef == null) {
            backingMapRef = "q:" + name;
        }
        return this;
    }

    /**
     * @return the maxSizePerJVM
     */
    public int getMaxSizePerJVM() {
        return maxSizePerJVM;
    }

    /**
     * @param maxSizePerJVM the maxSizePerJVM to set
     */
    public QueueConfig setMaxSizePerJVM(int maxSizePerJVM) {
        if (maxSizePerJVM < 0) {
            throw new IllegalArgumentException("queue max size per JVM must be positive");
        }
        this.maxSizePerJVM = maxSizePerJVM;
        return this;
    }

    public String getBackingMapRef() {
        return backingMapRef;
    }

    public QueueConfig setBackingMapRef(String backingMapRef) {
        this.backingMapRef = backingMapRef;
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
        return (name != null ? name.equals(queueConfig.name) : queueConfig.name == null) &&
                this.backingMapRef.equals(queueConfig.backingMapRef) &&
                this.maxSizePerJVM == queueConfig.maxSizePerJVM;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(backingMapRef);
        out.writeInt(maxSizePerJVM);
    }

    public void readData(DataInput in) throws IOException {
        name = in.readUTF();
        backingMapRef = in.readUTF();
        maxSizePerJVM = in.readInt();
    }

    @Override
    public String toString() {
        return "QueueConfig [name=" + this.name
                + ", backingMapRef=" + this.backingMapRef
                + ", maxSizePerJVM=" + this.maxSizePerJVM + "]";
    }
}
