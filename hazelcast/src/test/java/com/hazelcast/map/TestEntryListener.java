/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicBoolean;

class TestEntryListener implements EntryListener, HazelcastInstanceAware {

    static final AtomicBoolean INSTANCE_AWARE = new AtomicBoolean();

    @Override
    public void entryAdded(EntryEvent event) {
    }

    @Override
    public void entryEvicted(EntryEvent event) {
    }

    @Override
    public void entryExpired(EntryEvent event) {

    }

    @Override
    public void entryRemoved(EntryEvent event) {
    }

    @Override
    public void entryUpdated(EntryEvent event) {
    }

    @Override
    public void mapCleared(MapEvent event) {
    }

    @Override
    public void mapEvicted(MapEvent event) {
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        INSTANCE_AWARE.set(hazelcastInstance != null);
    }
}
