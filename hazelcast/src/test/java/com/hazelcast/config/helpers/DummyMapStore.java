/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.helpers;

import com.hazelcast.core.MapStore;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class DummyMapStore implements MapStore<Object, Object> {

    public DummyMapStore() {
    }

    @Override
    public void store(Object key, Object value) {
    }

    @Override
    public void storeAll(Map<Object, Object> map) {
    }

    @Override
    public void delete(Object key) {
    }

    @Override
    public void deleteAll(Collection<Object> keys) {
    }

    @Override
    public Object load(Object key) {
        return null;
    }

    @Override
    public Map<Object, Object> loadAll(Collection<Object> keys) {
        return null;
    }

    @Override
    public Set<Object> loadAllKeys() {
        return null;
    }
}
