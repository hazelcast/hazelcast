/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Empty map data store for providing neutral null behaviour.
 */
class EmptyMapDataStore implements MapDataStore {

    @Override
    public Object add(Object key, Object value, long now) {
        return value;
    }

    @Override
    public void addTransient(Object key, long now) {
    }

    @Override
    public Object addBackup(Object key, Object value, long now) {
        return value;
    }

    @Override
    public void remove(Object key, long now) {
    }

    @Override
    public void removeBackup(Object key, long now) {
    }

    @Override
    public void reset() {
    }

    @Override
    public Object load(Object key) {
        return null;
    }

    @Override
    public Map loadAll(Collection keys) {
        return Collections.emptyMap();
    }

    @Override
    public void removeAll(Collection keys) {
    }

    @Override
    public boolean loadable(Object key) {
        return false;
    }

    @Override
    public long softFlush() {
        return 0;
    }

    @Override
    public void hardFlush() {

    }

    @Override
    public Object flush(Object key, Object value, boolean backup) {
        return value;
    }

    @Override
    public int notFinishedOperationsCount() {
        return 0;
    }

    @Override
    public boolean isPostProcessingMapStore() {
        return false;
    }
}
