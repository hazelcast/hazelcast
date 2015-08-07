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

package com.hazelcast.map;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.MapEvent;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestEntryListener implements EntryListener, HazelcastInstanceAware {

    public static final AtomicInteger EVENT_COUNT = new AtomicInteger();
    public static final AtomicBoolean INSTANCE_AWARE = new AtomicBoolean();

    private HazelcastInstance instance;

    @Override
    public void entryAdded(EntryEvent event) {
        EVENT_COUNT.incrementAndGet();
    }

    @Override
    public void entryEvicted(EntryEvent event) {
        EVENT_COUNT.incrementAndGet();
    }

    @Override
    public void entryRemoved(EntryEvent event) {
        EVENT_COUNT.incrementAndGet();
    }

    @Override
    public void entryUpdated(EntryEvent event) {
        EVENT_COUNT.incrementAndGet();
    }

    @Override
    public void mapCleared(MapEvent event) {
        EVENT_COUNT.incrementAndGet();
    }

    @Override
    public void mapEvicted(MapEvent event) {
        EVENT_COUNT.incrementAndGet();
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.instance = hazelcastInstance;
        INSTANCE_AWARE.set(this.instance != null);
    }
}
