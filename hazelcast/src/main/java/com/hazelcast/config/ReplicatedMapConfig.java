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

import java.util.concurrent.ExecutorService;

public class ReplicatedMapConfig {

    private String name;
    private int concurrencyLevel = 32;
    private long replicationDelayMillis;
    private InMemoryFormat inMemoryFormat;
    private ExecutorService replicatorExecutorService;

    public ReplicatedMapConfig() {
    }

    public ReplicatedMapConfig(ReplicatedMapConfig replicatedMapConfig) {
        this.name = replicatedMapConfig.name;
        this.inMemoryFormat = replicatedMapConfig.inMemoryFormat;
        this.concurrencyLevel = replicatedMapConfig.concurrencyLevel;
        this.replicationDelayMillis = replicatedMapConfig.replicationDelayMillis;
        this.replicatorExecutorService = replicatedMapConfig.replicatorExecutorService;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getReplicationDelayMillis() {
        return replicationDelayMillis;
    }

    public void setReplicationDelayMillis(long replicationDelayMillis) {
        this.replicationDelayMillis = replicationDelayMillis;
    }

    public int getConcurrencyLevel() {
        return concurrencyLevel;
    }

    public void setConcurrencyLevel(int concurrencyLevel) {
        this.concurrencyLevel = concurrencyLevel;
    }

    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    public void setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        this.inMemoryFormat = inMemoryFormat;
    }

    public ExecutorService getReplicatorExecutorService() {
        return replicatorExecutorService;
    }

    public void setReplicatorExecutorService(ExecutorService replicatorExecutorService) {
        this.replicatorExecutorService = replicatorExecutorService;
    }

    public ReplicatedMapConfig getAsReadOnly() {
        return new ReplicatedMapConfigReadOnly(this);
    }

}
