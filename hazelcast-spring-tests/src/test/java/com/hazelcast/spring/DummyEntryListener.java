/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

public class DummyEntryListener implements EntryListener<Object, Object> {

    @Override
    public void entryAdded(EntryEvent<Object, Object> event) {
    }

    @Override
    public void entryRemoved(EntryEvent<Object, Object> event) {
    }

    @Override
    public void entryUpdated(EntryEvent<Object, Object> event) {
    }

    @Override
    public void entryEvicted(EntryEvent<Object, Object> event) {
    }

    @Override
    public void entryExpired(EntryEvent<Object, Object> event) {
    }

    @Override
    public void mapEvicted(MapEvent event) {
    }

    @Override
    public void mapCleared(MapEvent event) {
    }
}
