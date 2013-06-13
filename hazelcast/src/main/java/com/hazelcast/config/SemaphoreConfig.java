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

public class SemaphoreConfig {

    public final static int DEFAULT_SYNC_BACKUP_COUNT = 1;
    public final static int DEFAULT_ASYNC_BACKUP_COUNT = 0;

    private String name;
    private int initialPermits;
    private int syncBackupCount = DEFAULT_SYNC_BACKUP_COUNT;
    private int asyncBackupCount = DEFAULT_ASYNC_BACKUP_COUNT;


    public SemaphoreConfig() {
    }

    public SemaphoreConfig(SemaphoreConfig config) {
        this.name = config.getName();
        this.initialPermits = config.getInitialPermits();
        this.syncBackupCount = config.getSyncBackupCount();
        this.asyncBackupCount = config.getAsyncBackupCount();
    }

    public String getName() {
        return name;
    }

    public SemaphoreConfig setName(String name) {
        this.name = name;
        return this;
    }

    public int getInitialPermits() {
        return initialPermits;
    }

    public SemaphoreConfig setInitialPermits(int initialPermits) {
        this.initialPermits = initialPermits;
        return this;
    }

    public int getSyncBackupCount() {
        return syncBackupCount;
    }

    public SemaphoreConfig setSyncBackupCount(int syncBackupCount) {
        this.syncBackupCount = syncBackupCount;
        return this;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public SemaphoreConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = asyncBackupCount;
        return this;
    }

    public int getTotalBackupCount(){
        return asyncBackupCount + syncBackupCount;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SemaphoreConfig{");
        sb.append("name='").append(name).append('\'');
        sb.append(", initialPermits=").append(initialPermits);
        sb.append(", syncBackupCount=").append(syncBackupCount);
        sb.append(", asyncBackupCount=").append(asyncBackupCount);
        sb.append('}');
        return sb.toString();
    }
}
