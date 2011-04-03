/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.config;

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class QueueConfig implements DataSerializable {

    public final static int DEFAULT_MAX_SIZE_PER_JVM = 0;

    private String name;
    private String backingMapName;
    private int maxSizePerJVM = DEFAULT_MAX_SIZE_PER_JVM;

    public QueueConfig() {
    }

    public QueueConfig(QueueConfig config) {
        this.name = config.name;
        this.backingMapName = config.backingMapName;
        this.maxSizePerJVM = config.maxSizePerJVM;
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
        if (backingMapName == null) {
            backingMapName = "q:" + name;
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

    public String getBackingMapName() {
        return backingMapName;
    }

    public QueueConfig setBackingMapName(String backingMapName) {
        this.backingMapName = backingMapName;
        return this;
    }

    public boolean isCompatible(final QueueConfig queueConfig) {
        if (queueConfig == null) return false;
        return (name != null ? name.equals(queueConfig.name) : queueConfig.name == null) &&
                this.backingMapName.equals(queueConfig.backingMapName) &&
                this.maxSizePerJVM == queueConfig.maxSizePerJVM;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(backingMapName);
        out.writeInt(maxSizePerJVM);
    }

    public void readData(DataInput in) throws IOException {
        name = in.readUTF();
        backingMapName = in.readUTF();
        maxSizePerJVM = in.readInt();
    }

    @Override
    public String toString() {
        return "QueueConfig [name=" + this.name
                + ", backingMapName=" + this.backingMapName
                + ", maxSizePerJVM=" + this.maxSizePerJVM + "]";
    }
}
