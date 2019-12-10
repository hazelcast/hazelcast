/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wan.impl;

import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.wan.WanEvent;
import com.hazelcast.wan.WanPublisher;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

class WanDummyPublisher implements WanPublisher, InternalWanPublisher {

    Queue<WanEvent> eventQueue = new ConcurrentLinkedQueue<>();

    @Override
    public void init(WanReplicationConfig wanReplicationConfig,
                     AbstractWanPublisherConfig wanPublisherConfig) {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void publishReplicationEvent(WanEvent event) {
        eventQueue.add(event);
    }

    @Override
    public void publishReplicationEventBackup(WanEvent event) {
    }

    @Override
    public void destroyMapData(String mapName) {

    }

    @Override
    public int removeWanEvents(int partitionId, String serviceName, String objectName, int count) {
        int size = eventQueue.size();
        eventQueue.clear();
        return size;
    }

    @Override
    public void republishReplicationEvent(WanEvent event) {
        publishReplicationEvent(event);
    }

    @Override
    public void doPrepublicationChecks() {
    }

    Queue<WanEvent> getEventQueue() {
        return eventQueue;
    }
}
