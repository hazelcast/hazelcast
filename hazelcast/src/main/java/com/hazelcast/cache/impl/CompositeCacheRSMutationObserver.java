/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl;

import com.hazelcast.internal.serialization.Data;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.EMPTY_LIST;

/**
 * Dispatches changes to registered collection
 * of cache record store mutation-observers.
 */
public class CompositeCacheRSMutationObserver implements CacheRSMutationObserver {

    private List<CacheRSMutationObserver> mutationObservers = EMPTY_LIST;

    public CompositeCacheRSMutationObserver() {
    }

    public void add(CacheRSMutationObserver mutationObserver) {
        if (mutationObservers == EMPTY_LIST) {
            mutationObservers = new ArrayList<>();
        }
        mutationObservers.add(mutationObserver);
    }

    @Override
    public void writeCreatedEvent(Data key, Object value) {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).writeCreatedEvent(key, value);
        }
    }

    @Override
    public void writeRemoveEvent(Data key, Object value) {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).writeRemoveEvent(key, value);
        }
    }

    @Override
    public void writeUpdateEvent(Data key, Object oldDataValue, Object value) {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).writeUpdateEvent(key, oldDataValue, value);
        }
    }

    @Override
    public void writeEvictEvent(Data key, Object value) {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).writeEvictEvent(key, value);
        }
    }

    @Override
    public void writeExpiredEvent(Data key, Object value) {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).writeExpiredEvent(key, value);
        }
    }

    @Override
    public void destroy() {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).destroy();
        }
    }
}
