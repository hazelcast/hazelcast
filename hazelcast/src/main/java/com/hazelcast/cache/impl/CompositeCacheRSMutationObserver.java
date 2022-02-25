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
    public void onCreate(Data key, Object value) {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).onCreate(key, value);
        }
    }

    @Override
    public void onRemove(Data key, Object value) {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).onRemove(key, value);
        }
    }

    @Override
    public void onUpdate(Data key, Object oldValue, Object value) {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).onUpdate(key, oldValue, value);
        }
    }

    @Override
    public void onEvict(Data key, Object value) {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).onEvict(key, value);
        }
    }

    @Override
    public void onExpire(Data key, Object value) {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).onExpire(key, value);
        }
    }

    @Override
    public void onDestroy() {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).onDestroy();
        }
    }
}
