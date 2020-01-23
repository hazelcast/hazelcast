/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.management;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanQueueFullBehavior;

public class MCWanBatchPublisherConfig {
    private String name;
    private String targetCluster;
    private String publisherId;
    private String endpoints;
    private int queueCapacity;
    private int batchSize;
    private int batchMaxDelayMillis;
    private int responseTimeoutMillis;
    private WanAcknowledgeType ackType;
    private WanQueueFullBehavior queueFullBehaviour;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTargetCluster() {
        return targetCluster;
    }

    public void setTargetCluster(String targetCluster) {
        this.targetCluster = targetCluster;
    }

    public String getPublisherId() {
        return publisherId;
    }

    public void setPublisherId(String publisherId) {
        this.publisherId = publisherId;
    }

    public String getEndpoints() {
        return endpoints;
    }

    public void setEndpoints(String endpoints) {
        this.endpoints = endpoints;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchMaxDelayMillis() {
        return batchMaxDelayMillis;
    }

    public void setBatchMaxDelayMillis(int batchMaxDelayMillis) {
        this.batchMaxDelayMillis = batchMaxDelayMillis;
    }

    public int getResponseTimeoutMillis() {
        return responseTimeoutMillis;
    }

    public void setResponseTimeoutMillis(int responseTimeoutMillis) {
        this.responseTimeoutMillis = responseTimeoutMillis;
    }

    public WanAcknowledgeType getAckType() {
        return ackType;
    }

    public void setAckType(WanAcknowledgeType ackType) {
        this.ackType = ackType;
    }

    public WanQueueFullBehavior getQueueFullBehaviour() {
        return queueFullBehaviour;
    }

    public void setQueueFullBehaviour(WanQueueFullBehavior queueFullBehaviour) {
        this.queueFullBehaviour = queueFullBehaviour;
    }

    @Override
    public String toString() {
        return "MCWanBatchPublisherConfig{"
                + "name='" + name + '\''
                + ", targetCluster='" + targetCluster + '\''
                + ", publisherId='" + publisherId + '\''
                + ", endpoints='" + endpoints + '\''
                + ", queueCapacity=" + queueCapacity
                + ", batchSize=" + batchSize
                + ", batchMaxDelayMillis=" + batchMaxDelayMillis
                + ", responseTimeoutMillis=" + responseTimeoutMillis
                + ", ackType=" + ackType
                + ", queueFullBehaviour=" + queueFullBehaviour
                + '}';
    }
}
