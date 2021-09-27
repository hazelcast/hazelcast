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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.record.Record;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.EMPTY_LIST;

class CompositeMutationObserver<R extends Record> implements MutationObserver<R> {

    private static final int DEFAULT_OBSERVER_COUNT = 4;

    private List<MutationObserver> mutationObservers = EMPTY_LIST;

    CompositeMutationObserver() {
    }

    public void add(MutationObserver<R> mutationObserver) {
        if (mutationObservers == EMPTY_LIST) {
            mutationObservers = new ArrayList<>(DEFAULT_OBSERVER_COUNT);
        }
        mutationObservers.add(mutationObserver);
    }

    @Override
    public void onClear() {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).onClear();
        }
    }

    @Override
    public void onPutRecord(Data key, R record,
                            Object oldValue, boolean backup) {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).onPutRecord(key, record, oldValue, backup);
        }
    }

    @Override
    public void onReplicationPutRecord(@Nonnull Data key,
                                       @Nonnull R record, boolean populateIndex) {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).onReplicationPutRecord(key, record, populateIndex);
        }
    }

    @Override
    public void onUpdateRecord(@Nonnull Data key, @Nonnull R record,
                               Object oldValue, Object newValue, boolean backup) {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).onUpdateRecord(key, record, oldValue, newValue, backup);
        }
    }

    @Override
    public void onRemoveRecord(Data key, R record) {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).onRemoveRecord(key, record);
        }
    }

    @Override
    public void onEvictRecord(Data key, R record) {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).onEvictRecord(key, record);
        }
    }

    @Override
    public void onLoadRecord(@Nonnull Data key, @Nonnull R record, boolean backup) {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).onLoadRecord(key, record, backup);
        }
    }

    @Override
    public void onDestroy(boolean isDuringShutdown, boolean internal) {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).onDestroy(isDuringShutdown, internal);
        }
    }

    @Override
    public void onReset() {
        for (int i = 0; i < mutationObservers.size(); i++) {
            mutationObservers.get(i).onReset();
        }
    }
}
