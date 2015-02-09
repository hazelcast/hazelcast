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

package com.hazelcast.spring;

import com.hazelcast.core.MapStore;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class DummyStore implements MapStore {
    public void store(final Object key, final Object value) {
        // not implemented
    }

    public void storeAll(final Map map) {
        // not implemented
    }

    public void delete(final Object key) {
        // not implemented
    }

    public void deleteAll(final Collection keys) {
        // not implemented
    }

    public Object load(final Object key) {
        // not implemented
        return null;
    }

    public Map loadAll(final Collection keys) {
        // not implemented
        return null;
    }

    public Set loadAllKeys() {
        // not implemented
        return null;
    }
}
