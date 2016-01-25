/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import java.util.List;
/**
 * Configuration for Wan target cluster
 */
public class WanTargetClusterConfig {

    private static final WanAcknowledgeType DEFAULT_ACK_TYPE = WanAcknowledgeType.ACK_ON_RECEIPT;
    private static final WANQueueFullBehavior QUEUE_FULL_BEHAVIOR = WANQueueFullBehavior.DISCARD_AFTER_MUTATION;
    private static final int DEFAULT_BATCH_SIZE = 500;
    private static final long DEFAULT_BATCH_MAX_DELAY_MILLIS = 1000;
    private static final int DEFAULT_QUEUE_CAPACITY = 10000;
    private static final long DEFAULT_RESPONSE_TIMEOUT_MILLIS = 60000;

    int batchSize = DEFAULT_BATCH_SIZE;
    long batchMaxDelayMillis = DEFAULT_BATCH_MAX_DELAY_MILLIS;
    int queueCapacity = DEFAULT_QUEUE_CAPACITY;
    long responseTimeoutMillis = DEFAULT_RESPONSE_TIMEOUT_MILLIS;

    String groupName = "dev";
    String groupPassword = "dev-pass";
    String replicationImpl;
    Object replicationImplObject;
    // ip:port
    List<String> endpoints;
    WanAcknowledgeType acknowledgeType = DEFAULT_ACK_TYPE;
    WANQueueFullBehavior queueFullBehavior = QUEUE_FULL_BEHAVIOR;

    public String getGroupName() {
        return groupName;
    }

    public WanTargetClusterConfig setGroupName(String groupName) {
        this.groupName = groupName;
        return this;
    }

    public String getGroupPassword() {
        return groupPassword;
    }

    public WanTargetClusterConfig setGroupPassword(String groupPassword) {
        this.groupPassword = groupPassword;
        return this;
    }

    public List<String> getEndpoints() {
        return endpoints;
    }

    public WanTargetClusterConfig setEndpoints(List<String> list) {
        endpoints = list;
        return this;
    }

    public WanTargetClusterConfig addEndpoint(String address) {
        if (endpoints == null) {
            endpoints = new ArrayList<String>(2);
        }
        endpoints.add(address);
        return this;
    }

    public String getReplicationImpl() {
        return replicationImpl;
    }

    public WanTargetClusterConfig setReplicationImpl(String replicationImpl) {
        this.replicationImpl = replicationImpl;
        return this;
    }

    public Object getReplicationImplObject() {
        return replicationImplObject;
    }

    public WanTargetClusterConfig setReplicationImplObject(Object replicationImplObject) {
        this.replicationImplObject = replicationImplObject;
        return this;
    }

    public WanAcknowledgeType getAcknowledgeType() {
        return acknowledgeType;
    }

    public void setAcknowledgeType(WanAcknowledgeType acknowledgeType) {
        this.acknowledgeType = acknowledgeType;
    }

    public WANQueueFullBehavior getQueueFullBehavior() {
        return queueFullBehavior;
    }

    public void setQueueFullBehavior(WANQueueFullBehavior queueFullBehavior) {
        this.queueFullBehavior = queueFullBehavior;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public long getBatchMaxDelayMillis() {
        return batchMaxDelayMillis;
    }

    public void setBatchMaxDelayMillis(long batchMaxDelayMillis) {
        this.batchMaxDelayMillis = batchMaxDelayMillis;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public long getResponseTimeoutMillis() {
        return responseTimeoutMillis;
    }

    public void setResponseTimeoutMillis(long responseTimeoutMillis) {
        this.responseTimeoutMillis = responseTimeoutMillis;
    }

    @Override
    public String toString() {
        return "WanTargetClusterConfig{"
                + "groupName='" + groupName + '\''
                + ", replicationImpl='" + replicationImpl + '\''
                + ", replicationImplObject=" + replicationImplObject
                + ", endpoints=" + endpoints
                + ", acknowledgeType=" + acknowledgeType
                + ", queueFullBehavior=" + queueFullBehavior
                + ", batchSize=" + batchSize
                + ", batchMaxDelayMillis=" + batchMaxDelayMillis
                + ", queueCapacity=" + queueCapacity
                + ", responseTimeoutMillis" + responseTimeoutMillis
                + '}';
    }
}
