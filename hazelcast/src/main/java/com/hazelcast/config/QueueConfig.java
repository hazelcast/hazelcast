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
    public final static int DEFAULT_TTL_SECONDS = 0;
    public final static int DEFAULT_BACKUP_COUNT = 1;

    private String name;

    private int maxSizePerJVM = DEFAULT_MAX_SIZE_PER_JVM;

    private int timeToLiveSeconds = DEFAULT_TTL_SECONDS;

    private int backupCount = DEFAULT_BACKUP_COUNT;

    public QueueConfig() {
    }

    public QueueConfig(QueueConfig config) {
        this.name = config.name;
        this.maxSizePerJVM = config.maxSizePerJVM;
        this.timeToLiveSeconds = config.timeToLiveSeconds;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public QueueConfig setName(String name) {
        this.name = name;
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

    /**
     * @return the timeToLiveSeconds
     */
    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    /**
     * Returns the number of backups for this queue.
     *
     * @return number of backups.
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Sets the number of backups for this queue. Default is 1.
     *
     * @param backupCount number of backups.
     * @return this queue config
     */
    public QueueConfig setBackupCount(int backupCount) {
        this.backupCount = backupCount;
        return this;
    }

    /**
     * @param timeToLiveSeconds the timeToLiveSeconds to set
     */
    public QueueConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
        if (timeToLiveSeconds < 0) {
            throw new IllegalArgumentException("queue TTL must be positive");
        }
        this.timeToLiveSeconds = timeToLiveSeconds;
        return this;
    }

    public boolean isCompatible(final QueueConfig queueConfig) {
        if (queueConfig == null) return false;
        return (name != null ? name.equals(queueConfig.name) : queueConfig.name == null) &&
                this.timeToLiveSeconds == queueConfig.timeToLiveSeconds;
    }

    @Override
    public String toString() {
        return "QueueConfig [name=" + this.name
                + ", timeToLiveSeconds=" + this.timeToLiveSeconds
                + ", maxSizePerJVM=" + this.maxSizePerJVM + "]";
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(maxSizePerJVM);
        out.writeInt(timeToLiveSeconds);
    }

    public void readData(DataInput in) throws IOException {
        name = in.readUTF();
        maxSizePerJVM = in.readInt();
        timeToLiveSeconds = in.readInt();
    }
}
