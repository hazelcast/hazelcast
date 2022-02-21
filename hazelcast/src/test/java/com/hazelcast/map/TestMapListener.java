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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;

import java.util.concurrent.atomic.AtomicBoolean;

class TestMapListener implements EntryAddedListener, EntryRemovedListener, HazelcastInstanceAware {

    static final AtomicBoolean INSTANCE_AWARE = new AtomicBoolean();

    @Override
    public void entryAdded(EntryEvent event) {
    }

    @Override
    public void entryRemoved(EntryEvent event) {
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        INSTANCE_AWARE.set(hazelcastInstance != null);
    }
}
