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

package com.hazelcast.datastream.impl;

import com.hazelcast.nio.Connection;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class DSPartitionSubscriptions {

    private final String name;
    private final DSService service;
    private final DSPartition partition;
    private final ArrayList<Subscription> subscriptions = new ArrayList<Subscription>();

    DSPartitionSubscriptions(DSService service, String name, DSPartition partition) {
        this.name = name;
        this.service = service;
        this.partition = partition;
    }

    public void register(String uuid, Connection connection, long offset){
        subscriptions.add(new Subscription(uuid, connection, offset));
    }

    public void onAppend(DSPartition partition){
        for(Subscription subscription: subscriptions){

        }
    }

    private class Subscription {
        private final String uuid;
        private long offset;
        private Connection connection;
        private final AtomicLong bytesInFlight;

        Subscription(String uuid, Connection connection, long offset){
           this.bytesInFlight  = service.getBytesInFlight(uuid);
           this.uuid = uuid;
           this.connection=connection;
           this.offset = offset;
        }
    }
}
