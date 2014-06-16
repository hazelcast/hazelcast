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

package com.hazelcast.spring;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.map.MapEvent;

public class DummyEntryListener implements EntryListener {

    public void entryAdded(EntryEvent event) {
//        System.err.println("Added: " + event);
    }

    public void entryRemoved(EntryEvent event) {
//        System.err.println("Removed: " + event);
    }

    public void entryUpdated(EntryEvent event) {
//        System.err.println("Updated: " + event);
    }

    public void entryEvicted(EntryEvent event) {
//        System.err.println("Evicted: " + event);
    }

    @Override
    public void mapEvicted(MapEvent event) {

    }
}
